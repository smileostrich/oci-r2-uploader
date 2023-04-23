use std::env;
use anyhow::{Context, Result};

pub struct R2Configs {
    pub cloudflare_account_id: String,
    pub r2_bucket: String,
    pub r2_access_key_id: String,
    pub r2_secret_access_key: String,
}

pub fn parse_r2configs() -> Result<R2Configs> {
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
