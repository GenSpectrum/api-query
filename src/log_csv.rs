use std::{
    fmt::{Display, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};

/// The api-query log file in CSV format
pub struct LogCsv {
    tmp: String,
    path: PathBuf,
    log_file: BufWriter<File>,
}

impl LogCsv {
    pub const NUM_COLS: usize = 6;
    pub const HEADER: [&str; Self::NUM_COLS] =
        ["line in query file", "start", "end", "d", "status", "crc"];

    pub async fn create(path: &Path) -> Result<Self> {
        let mut log_file = BufWriter::new(
            File::create(path)
                .await
                .with_context(|| anyhow!("opening {path:?} for writing"))?,
        );
        log_file
            .write_all("line in query file,start,end,d,status,crc\n".as_bytes())
            .await
            .context("writing to CSV log file")?;
        Ok(Self {
            tmp: String::new(),
            path: path.to_owned(),
            log_file,
        })
    }

    pub async fn write_row(&mut self, values: [&dyn Display; Self::NUM_COLS]) -> Result<()> {
        let Self {
            tmp,
            path,
            log_file,
        } = self;

        tmp.clear();
        let [a, b, c, d, e, f] = values;
        writeln!(tmp, "{a},{b},{c},{d},{e},\"crc:{f}\"",)?;

        log_file
            .write_all(tmp.as_bytes())
            .await
            .with_context(|| anyhow!("writing to CSV log file {path:?}"))?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.log_file
            .flush()
            .await
            .with_context(|| anyhow!("flushing CSV log file {:?}", self.path))?;
        Ok(())
    }
}
