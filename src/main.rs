use std::io::{stdin, Read, stdout, Write};

use reqwest::Response;
use tokio;
use anyhow::{Result, Context, anyhow};

#[tokio::main]
async fn main() -> Result<()> {
    let mut query = String::new();
    stdin().read_to_string(&mut query)
        .with_context(|| anyhow!("reading from stdin"))?;
    let client = reqwest::Client::new();
    let mut res: Response = client.post("http://localhost:8081/query")
        .body(query.clone())
        .send()
        .await
        .with_context(|| anyhow!("posting the query {query:?}"))?;
    // https://docs.rs/reqwest/0.12.2/reqwest/struct.Response.html
    while let Some(bytes) = res.chunk().await
        .with_context(|| anyhow!("reading the result from query {query:?}"))?
    {
        stdout().write_all(&bytes)
            .with_context(|| anyhow!("writing to stdout"))?;
    }
    if res.status() != 200 {
        stdout().write_all(b"\n")
            .with_context(|| anyhow!("writing to stdout"))?;
        std::process::exit(1);
    }
    Ok(())
}
