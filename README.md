# oci-r2-uploader

Rust library for converting and uploading Docker images to Cloudflare R2 Storage with customizable image and tag parameters.

## Features

- Convert Docker images to OCI format
- Upload Docker images to Cloudflare R2 Storage
- Customizable image and tag parameters

## Installation

Add the following dependency to your `Cargo.toml` file:

```toml
[dependencies]
docker-r2-uploader = "0.1.0"
```

## Usage

```rust
use oci_r2_uploader;
use anyhow::Result;

#[tokio::main]
async fn main() {
let image = String::from("my_image");
let tag = String::from("my_tag");

    if let Err(e) = docker_r2_uploader::run(image, tag).await {
    }
}
```

## License

This project is licensed under the MIT License.

## Contributing

1. Fork the repository
2. Create your feature branch (git checkout -b feature/my-feature)
3. Commit your changes (git commit -am 'Add my feature')
4. Push to the branch (git push origin feature/my-feature)
5. Create a new Pull Request
