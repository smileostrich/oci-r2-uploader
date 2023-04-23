use std::fs;

use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use std::path::{Path};
use anyhow::{Context, Result};
use serde_json::Value;

use crate::r2configs::R2Configs;

pub(crate) async fn upload_blobs(image: &str, image_blobs_dir: &Path, client: &S3Client, r2_bucket: &str) -> Result<()> {
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

pub(crate) async fn upload_manifests(image: &str, image_manifests_dir: &Path, client: &S3Client, r2_bucket: &str) -> Result<()> {
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

pub(crate) fn prepare_s3_client(env_vars: &R2Configs) -> Result<S3Client> {
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
