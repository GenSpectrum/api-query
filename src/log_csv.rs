use std::{
    fs::File,
    io::BufWriter,
    path::Path,
    sync::{mpsc, Arc},
    thread,
};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::StatusCode;

use crate::{my_crc::Crc, time::UnixTimeWrap, types::QueryReference};

pub enum LogCsvResult {
    Ok(StatusCode, Crc),
    Err(String),
}

pub struct LogCsvRecord(
    /// Reference (line number) into the queries file
    pub QueryReference,
    /// Repetition
    pub u32,
    /// Start time of the query
    pub UnixTimeWrap,
    /// End time of the query
    pub UnixTimeWrap,
    /// The time difference
    pub f64,
    /// LogCsvResult is yielding 4 columns in the CSV file
    pub LogCsvResult,
);

/// The api-query log file in CSV format
struct LogCsv {
    path: Arc<Path>,
    writer: csv::Writer<BufWriter<File>>,
}

impl LogCsv {
    const NUM_COLS: usize = 9;
    const HEADER: [&str; Self::NUM_COLS] = [
        "line in query file",
        "repetition",
        "start",
        "end",
        "d",
        "Ok/Err",
        "status",
        "crc",
        "error",
    ];

    fn create(path: Arc<Path>) -> Result<Self> {
        let log_file = BufWriter::new(
            File::create(&*path).with_context(|| anyhow!("opening {path:?} for writing"))?,
        );

        let mut writer = csv::Writer::from_writer(log_file);
        writer
            .write_record(Self::HEADER)
            .with_context(|| anyhow!("writing to CSV log file {path:?}"))?;

        Ok(Self { path, writer })
    }

    fn write_row(&mut self, values: LogCsvRecord) -> Result<()> {
        let Self { path, writer } = self;

        let LogCsvRecord(a, b, c, d, e, res) = values;
        // lame, wanted to avoid allocations, but there we are (and I
        // don't want to write serde serializers).
        let mut record = [
            a.to_string(),
            b.to_string(),
            c.to_string(),
            d.to_string(),
            e.to_string(),
            String::new(), // 5
            String::new(),
            String::new(),
            String::new(),
        ];
        match res {
            LogCsvResult::Ok(status_code, crc) => {
                record[5] = "Ok".into();
                record[6] = status_code.to_string();
                record[7] = crc.to_string();
            }
            LogCsvResult::Err(e) => {
                record[5] = "Err".into();
                record[8] = e;
            }
        }

        writer
            .write_record(record)
            .with_context(|| anyhow!("writing to CSV log file {path:?}"))?;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .with_context(|| anyhow!("flushing CSV log file {:?}", self.path))?;
        Ok(())
    }
}

/// Log writer in a separate thread, with the writing end of a channel
/// for sending it log messages.
pub struct LogCsvWriter {
    thread: thread::JoinHandle<Result<()>>,
    channel_tx: mpsc::Sender<LogCsvRecord>,
    path: Arc<Path>,
}

impl LogCsvWriter {
    /// Create a log writer running in a separate thread.
    pub fn create(path: Arc<Path>) -> Result<Self> {
        let mut log_file = LogCsv::create(path.clone())?;
        let (channel_tx, channel_rx) = mpsc::channel();
        let thread = thread::spawn(move || -> Result<()> {
            for entry in channel_rx {
                log_file.write_row(entry)?;
            }
            log_file.flush()
        });
        Ok(Self {
            thread,
            channel_tx,
            path,
        })
    }

    /// Send a log record to the writer thread / log.
    pub fn send(&self, record: LogCsvRecord) -> Result<()> {
        self.channel_tx
            .send(record)
            .with_context(|| anyhow!("sending to CSV log writer for file {:?}", self.path))
    }

    /// Finish writing and flushing all buffered messages.
    pub fn finish(self) -> Result<()> {
        let Self {
            thread,
            channel_tx,
            path,
        } = self;
        drop(channel_tx);
        match thread.join() {
            Ok(v) => v.with_context(|| anyhow!("CSV log writer thread for file {path:?}")),
            Err(e) => bail!("CSV log writer thread for file {path:?} panicked: {e:?}"),
        }
    }
}
