use std::env;
use std::fs;
use std::io::{Read};
use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use serde_json::Value;

use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};


pub async fn run(image: String, tag: String) -> Result<()> {
    let script_dir = Path::new("--").parent().unwrap().to_owned();
    let tmp_dir = TempDir::new_in(&script_dir)?;

    check_command_installed("skopeo")?;

    let cloudflare_account_id = env::var("CLOUDFLARE_ACCOUNT_ID").context("CLOUDFLARE_ACCOUNT_ID is not set")?;
    let r2_bucket = env::var("R2_BUCKET").context("R2_BUCKET is not set")?;
    let r2_access_key_id = env::var("R2_ACCESS_KEY_ID").context("R2_ACCESS_KEY_ID is not set")?;
    let r2_secret_access_key = env::var("R2_SECRET_ACCESS_KEY").context("R2_SECRET_ACCESS_KEY is not set")?;

    let status = Command::new("skopeo")
        .arg("copy")
        .arg("--all")
        .arg(format!("docker-daemon:{}:{}", image, tag))
        .arg(format!("dir:{}", tmp_dir.path().display()))
        .status()?;

    if !status.success() {
        bail!("Failed to convert image");
    }

    let v2_dir = script_dir.join("v2");
    fs::create_dir_all(&v2_dir)?;

    let image_manifests_dir = v2_dir.join(&image).join("manifests");
    let image_blobs_dir = v2_dir.join(&image).join("blobs");
    fs::create_dir_all(&image_manifests_dir)?;
    fs::create_dir_all(&image_blobs_dir)?;

    for entry in fs::read_dir(tmp_dir.path())? {
        let entry = entry?;
        let src = entry.path();
        let file_name = src.file_name().unwrap().to_string_lossy().into_owned();

        if file_name == "version" {
            fs::remove_file(src)?;
            continue;
        }

        let dst_dir = if file_name.ends_with(".manifest.json") {
            &image_manifests_dir
        } else {
            &image_blobs_dir
        };

        let sha = compute_sha256(&src)?;
        let dst = dst_dir.join(sha);
        fs::rename(&src, &dst)?;
    }

    let s3_endpoint = format!("https://{}.r2.cloudflarestorage.com", cloudflare_account_id);

    let region = Region::Custom {
        name: "auto".to_owned(),
        endpoint: s3_endpoint.clone(),
    };

    let client = S3Client::new_with(
        rusoto_core::HttpClient::new().expect("failed to create request dispatcher"),
        rusoto_core::credential::StaticProvider::new_minimal(
            r2_access_key_id.clone(),
            r2_secret_access_key.clone(),
        ),
        region,
    );

    for entry in fs::read_dir(&image_blobs_dir)? {
        let entry = entry?;
        let blob = entry.path();
        let blob_name = blob.file_name().unwrap().to_str().unwrap();

        let key = format!("v2/{}/blobs/{}", image, blob_name);
        let blob_data = fs::read(blob.clone())?;

        let req = PutObjectRequest {
            bucket: r2_bucket.clone(),
            key: key.clone(),
            body: Some(blob_data.into()),
            content_type: Some("application/octet-stream".to_owned()),
            ..Default::default()
        };

        match client.put_object(req).await {
            Ok(_) => {
                log::info!("Uploaded blob {}", blob_name);
            }
            Err(e) => {
                bail!("Failed to upload blob {}: {:?}", blob_name, e);
            }
        }
    }

    for entry in fs::read_dir(&image_manifests_dir)? {
        let entry = entry?;
        let manifest = entry.path();
        let manifest_name = manifest.file_name().unwrap().to_str().unwrap();

        let manifest_data = fs::read_to_string(&manifest)?;
        let manifest_json: Value = serde_json::from_str(&manifest_data)?;
        let content_type = manifest_json["mediaType"].as_str().unwrap().to_owned();

        let key = format!("v2/{}/manifests/{}", image, manifest_name);

        let req = PutObjectRequest {
            bucket: r2_bucket.clone(),
            key: key.clone(),
            body: Some(manifest_data.into_bytes().into()),
            content_type: Some(content_type),
            ..Default::default()
        };

        match client.put_object(req).await {
            Ok(_) => {
                log::info!("Uploaded manifest {}", manifest_name);
            }
            Err(e) => {
                bail!("Failed to upload manifest {}: {:?}", manifest_name, e);
            }
        }
    }

    tmp_dir.close()?;
    fs::remove_dir_all(&v2_dir)?;

    Ok(())
}

fn check_command_installed(cmd: &str) -> Result<()> {
    if Command::new(cmd).output().is_err() {
        bail!("{} is not installed", cmd);
    }

    Ok(())
}

fn compute_sha256<P: AsRef<Path>>(path: P) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut sha = Sha256::new();
    let mut buffer = [0; 4096];
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }

        sha.update(&buffer[..bytes]);
    }

    Ok(format!("{:x}", sha.finalize()))
}
