use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use blake3::Hasher;
use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use serde_json::Value;
use tempfile::TempDir;

pub async fn run(image: String, tag: String) -> Result<()> {
    let script_dir = Path::new("--").parent().unwrap().to_owned();
    let tmp_dir = TempDir::new_in(&script_dir)?;

    check_skopeo("skopeo")?;

    let env_vars = get_required_environment_variables()?;

    let status = convert_oci(&image, &tag, &tmp_dir)?;
    if !status.success() {
        bail!("Failed to convert image");
    }

    let (image_manifests_dir, image_blobs_dir) = prepare_dir(&script_dir, &image)?;

    move_files(&tmp_dir, &image_manifests_dir, &image_blobs_dir)?;

    let client = prepare_s3_client(&env_vars)?;

    upload_blobs(&image, &image_blobs_dir, &client, &env_vars.r2_bucket).await?;

    upload_manifests(&image, &image_manifests_dir, &client, &env_vars.r2_bucket).await?;

    cleanup(tmp_dir, &script_dir, &image)?;

    Ok(())
}

fn check_skopeo(cmd: &str) -> Result<()> {
    if Command::new(cmd).output().is_err() {
        bail!("{} is not installed", cmd);
    }

    Ok(())
}

fn compute_blake3<P: AsRef<Path>>(path: P) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buffer = [0; 4096];
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }

        hasher.update(&buffer[..bytes]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

struct R2Configs {
    cloudflare_account_id: String,
    r2_bucket: String,
    r2_access_key_id: String,
    r2_secret_access_key: String,
}

fn get_required_environment_variables() -> Result<R2Configs> {
    let cloudflare_account_id = env::var("CLOUDFLARE_ACCOUNT_ID").context("CLOUDFLARE_ACCOUNT_ID is not set")?;
    let r2_bucket = env::var("R2_BUCKET").context("R2_BUCKET is not set")?;
    let r2_access_key_id = env::var("R2_ACCESS_KEY_ID").context("R2_ACCESS_KEY_ID is not set")?;
    let r2_secret_access_key = env::var("R2_SECRET_ACCESS_KEY").context("R2_SECRET_ACCESS_KEY is not set")?;

    Ok(R2Configs {
        cloudflare_account_id,
        r2_bucket,
        r2_access_key_id,
        r2_secret_access_key,
    })
}

fn convert_oci(image: &str, tag: &str, tmp_dir: &TempDir) -> Result<std::process::ExitStatus> {
    Command::new("skopeo")
        .arg("copy")
        .arg("--all")
        .arg(format!("docker-daemon:{}:{}", image, tag))
        .arg(format!("dir:{}", tmp_dir.path().display()))
        .status()
        .context("Failed to execute skopeo command")
}

fn prepare_dir(script_dir: &Path, image: &str) -> Result<(PathBuf, PathBuf)> {
    let v2_dir = script_dir.join("v2");
    fs::create_dir_all(&v2_dir)?;

    let image_manifests_dir = v2_dir.join(&image).join("manifests");
    let image_blobs_dir = v2_dir.join(&image).join("blobs");
    fs::create_dir_all(&image_manifests_dir)?;
    fs::create_dir_all(&image_blobs_dir)?;

    Ok((image_manifests_dir, image_blobs_dir))
}

fn move_files(tmp_dir: &TempDir, image_manifests_dir: &Path, image_blobs_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(tmp_dir.path())? {
        let src = entry?.path();
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

        let hash = compute_blake3(&src)?;
        let dst = dst_dir.join(hash);

        fs::rename(&src, &dst)?;
    }

    Ok(())
}

fn prepare_s3_client(env_vars: &R2Configs) -> Result<S3Client> {
    let s3_endpoint = format!("https://{}.r2.cloudflarestorage.com", env_vars.cloudflare_account_id);

    let region = Region::Custom {
        name: "auto".to_owned(),
        endpoint: s3_endpoint,
    };

    Ok(S3Client::new_with(
        rusoto_core::HttpClient::new().expect("failed to create request dispatcher"),
        rusoto_core::credential::StaticProvider::new_minimal(
            env_vars.r2_access_key_id.clone(),
            env_vars.r2_secret_access_key.clone(),
        ),
        region,
    ))
}

async fn upload_blobs(image: &str, image_blobs_dir: &Path, client: &S3Client, r2_bucket: &str) -> Result<()> {
    for entry in fs::read_dir(&image_blobs_dir)? {
        let entry = entry?;
        let blob = entry.path();
        let blob_name = blob.file_name().unwrap().to_str().unwrap();

        let key = format!("v2/{}/blobs/{}", image, blob_name);
        let blob_data = fs::read(blob.clone())?;

        let req = PutObjectRequest {
            bucket: r2_bucket.to_owned(),
            key: key.clone(),
            body: Some(blob_data.into()),
            content_type: Some("application/octet-stream".to_owned()),
            ..Default::default()
        };

        client.put_object(req).await.context(format!("Failed to upload blob {}", blob_name))?;
        log::info!("Uploaded blob {}", blob_name);
    }

    Ok(())
}

async fn upload_manifests(image: &str, image_manifests_dir: &Path, client: &S3Client, r2_bucket: &str) -> Result<()> {
    for entry in fs::read_dir(&image_manifests_dir)? {
        let entry = entry?;
        let manifest = entry.path();
        let manifest_name = manifest.file_name().unwrap().to_str().unwrap();

        let manifest_data = fs::read_to_string(&manifest)?;
        let manifest_json: Value = serde_json::from_str(&manifest_data)?;
        let content_type = manifest_json["mediaType"].as_str().unwrap().to_owned();

        let key = format!("v2/{}/manifests/{}", image, manifest_name);

        let req = PutObjectRequest {
            bucket: r2_bucket.to_owned(),
            key: key.clone(),
            body: Some(manifest_data.into_bytes().into()),
            content_type: Some(content_type),
            ..Default::default()
        };

        client.put_object(req).await.context(format!("Failed to upload manifest {}", manifest_name))?;
        log::info!("Uploaded manifest {}", manifest_name);
    }

    Ok(())
}

fn cleanup(tmp_dir: TempDir, script_dir: &Path, image: &str) -> Result<()> {
    tmp_dir.close()?;
    let v2_dir = script_dir.join("v2").join(image);
    fs::remove_dir_all(&v2_dir)?;

    Ok(())
}
