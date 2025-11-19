use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use itertools::Itertools;
use reqwest::StatusCode;

use crate::{my_crc::Crc, time::UnixTimeWrap, types::QueryReference};

pub struct LogCsvRecord(
    pub QueryReference,
    pub UnixTimeWrap,
    pub UnixTimeWrap,
    pub f64,
    pub StatusCode,
    pub Crc,
);

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

    pub fn create(path: &Path) -> Result<Self> {
        let mut log_file = BufWriter::new(
            File::create(path).with_context(|| anyhow!("opening {path:?} for writing"))?,
        );
        (|| -> Result<()> {
            for row in itertools::Itertools::intersperse(Self::HEADER.iter(), &",") {
                log_file.write_all(row.as_bytes())?;
            }
            log_file.write_all(b"\n")?;
            Ok(())
        })()
        .with_context(|| anyhow!("writing to {path:?}"))?;
        Ok(Self {
            tmp: String::new(),
            path: path.to_owned(),
            log_file,
        })
    }

    pub fn write_row(&mut self, values: LogCsvRecord) -> Result<()> {
        let Self {
            tmp,
            path,
            log_file,
        } = self;

        tmp.clear();
        let LogCsvRecord(a, b, c, d, e, f) = values;
        use std::fmt::Write;
        writeln!(tmp, "{a},{b},{c},{d},{e},\"crc:{f}\"",)?;

        log_file
            .write_all(tmp.as_bytes())
            .with_context(|| anyhow!("writing to CSV log file {path:?}"))?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.log_file
            .flush()
            .with_context(|| anyhow!("flushing CSV log file {:?}", self.path))?;
        Ok(())
    }
}
