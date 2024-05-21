use std::io::{stdin, Read, stdout, Write};

use reqwest::Response;
use tokio;
use anyhow::{Result, Context, anyhow};

fn getenv(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(s) => Ok(Some(s)),
        Err(e) => match e {
            std::env::VarError::NotPresent => Ok(None),
            std::env::VarError::NotUnicode(_) => Err(e)?
        }
    }
}

fn usage(cmd: &str) -> ! {
    eprintln!("usage: {cmd} [endpoint_url]");
    eprintln!("  The PORT env var can be set to modify the default url.");
    std::process::exit(1);
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args();
    let cmd = args.next().unwrap();
    let args: Vec<String> = args.collect();

    let endpoint_url =
        match args.len() {
            0 =>
                if let Some(url) = getenv("ENDPOINT_URL")? {
                    url
                } else {
                    let portstr = getenv("PORT")?.unwrap_or_else(|| "8081".into());
                    let port: u16 =
                        portstr.parse()
                        .with_context(
                            || anyhow!("parsing port string {portstr:?} from PORT env var"))?;
                    format!("http://localhost:{port}/query")
                },
            1 =>
                match &*args[0] {
                    "-h" | "--help" => usage(&cmd),
                    v => v.into()
                },
            _ => usage(&cmd)
        };
 
    let mut query = String::new();
    stdin().read_to_string(&mut query)
        .with_context(|| anyhow!("reading from stdin"))?;
    let client = reqwest::Client::new();
    let mut res: Response = client.post(endpoint_url)
        .header("Connection", "keep-alive") // should be default anyway, but silo doesn't do it
        .body(query.clone())
        .send()
        .await
        .with_context(|| anyhow!("posting the query {query:?}"))?;
    let mut out = stdout().lock();
    // https://docs.rs/reqwest/0.12.2/reqwest/struct.Response.html
    while let Some(bytes) = res.chunk().await
        .with_context(|| anyhow!("reading the result from query {query:?}"))?
    {
        out.write_all(&bytes)
            .with_context(|| anyhow!("writing to stdout"))?;
    }
    if res.status() != 200 {
        out.write_all(b"\n")
            .with_context(|| anyhow!("writing to stdout"))?;
        std::process::exit(1);
    }
    out.flush()?;
    Ok(())
}
