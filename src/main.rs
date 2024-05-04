use std::io::{stdin, Read};

use reqwest::Response;
use tokio;
use anyhow::{Result, Context, anyhow};

#[tokio::main]
async fn main() -> Result<()> {
    let mut query = String::new();
    stdin().read_to_string(&mut query)
        .with_context(|| anyhow!("reading from stdin"))?;
    let client = reqwest::Client::new();
    let res: Response = client.post("http://localhost:8081/query")
        .body(query)
        .send()
        .await?;
    // https://docs.rs/reqwest/0.12.2/reqwest/struct.Response.html
    let s = res.text().await?;
    println!("{s}");
    Ok(())
}
