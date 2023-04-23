mod r2configs;
mod v2;
mod hash_utils;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use anyhow::{bail, Context, Result};
use tempfile::TempDir;

pub async fn run(image: String, tag: String) -> Result<()> {
    let script_dir = Path::new("--").parent().unwrap().to_owned();
    let tmp_dir = TempDir::new_in(&script_dir)?;

    check_skopeo("skopeo")?;

    let env_vars = r2configs::parse_r2configs()?;

    let status = convert_oci(&image, &tag, &tmp_dir)?;
    if !status.success() {
        bail!("Failed to convert image");
    }

    let (image_manifests_dir, image_blobs_dir) = prepare_dir(&script_dir, &image)?;

    move_files(&tmp_dir, &image_manifests_dir, &image_blobs_dir)?;

    let client = v2::s3_upload::prepare_s3_client(&env_vars)?;

    v2::s3_upload::upload_blobs(&image, &image_blobs_dir, &client, &env_vars.r2_bucket).await?;

    v2::s3_upload::upload_manifests(&image, &image_manifests_dir, &client, &env_vars.r2_bucket).await?;

    cleanup(tmp_dir, &script_dir, &image)?;

    Ok(())
}

fn check_skopeo(cmd: &str) -> Result<()> {
    if Command::new(cmd).output().is_err() {
        bail!("{} is not installed", cmd);
    }

    Ok(())
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

        let hash = hash_utils::compute_blake3(&src)?;
        let dst = dst_dir.join(hash);

        fs::rename(&src, &dst)?;
    }

    Ok(())
}

fn cleanup(tmp_dir: TempDir, script_dir: &Path, image: &str) -> Result<()> {
    tmp_dir.close()?;
    let v2_dir = script_dir.join("v2").join(image);
    fs::remove_dir_all(&v2_dir)?;

    Ok(())
}
