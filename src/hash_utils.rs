use std::fs;
use std::io::Read;
use std::path::Path;
use anyhow::Result;
use blake3::Hasher;

pub fn compute_blake3<P: AsRef<Path>>(path: P) -> Result<String> {
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
