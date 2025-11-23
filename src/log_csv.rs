use std::{
    convert::TryInto,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
    sync::{mpsc, Arc},
    thread,
};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::StatusCode;

use crate::{
    my_crc::Crc,
    time::UnixTimeWrap,
    types::{QueryReference, QueryReferenceWithRepetition},
    vec_backing::RefVecBacking,
};

/// The result of a query
#[derive(Debug)]
pub enum LogCsvResult {
    Ok(StatusCode, Crc),
    Err(String),
}

/// A log entry
#[derive(Debug)]
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

impl LogCsvRecord {
    pub fn query_reference(&self) -> QueryReference {
        self.0
    }
    pub fn repetition(&self) -> u32 {
        self.1
    }
    pub fn query_reference_with_repetition(&self) -> QueryReferenceWithRepetition {
        QueryReferenceWithRepetition {
            query_reference: self.query_reference(),
            repetition: self.repetition(),
        }
    }
    pub fn result(&self) -> &LogCsvResult {
        &self.5
    }
    /// The CRC when there is one (non-error cases). Note: disregards
    /// the status!
    pub fn crc(&self) -> Option<Crc> {
        match self.result() {
            LogCsvResult::Ok(_status_code, crc) => Some(*crc),
            LogCsvResult::Err(_) => None,
        }
    }
}

const NUM_COLS: usize = 9;
const HEADER: [&str; NUM_COLS] = [
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

pub fn parse_row(row: &[impl AsRef<str>; NUM_COLS]) -> Result<LogCsvRecord> {
    let [line, repetition, start, end, d, ok_err, status_code, crc, error] = row;

    macro_rules! let_parse {
        { $var:ident ? $msg:expr } =>  {
            let $var: &str = $var.as_ref();
            let $var = $var.parse().with_context(|| anyhow!(
                "parsing field value {:?} as {}",
                $var,
                $msg
            ))?;
        }
    }

    let_parse!(line ? "line in query file");
    let_parse!(repetition ? "repetition");
    let_parse!(start ? "start");
    let_parse!(end ? "end");
    let_parse!(d ? "d");

    let ok_err = ok_err.as_ref();
    match ok_err {
        "Ok" => {
            let (status_code, _) = status_code
                .as_ref()
                .split_once(' ')
                .ok_or_else(|| anyhow!("expecting status code number followed by a space"))?;

            let_parse!(status_code ? "HTTP status code");
            let_parse!(crc ? "CRC");

            Ok(LogCsvRecord(
                line,
                repetition,
                start,
                end,
                d,
                LogCsvResult::Ok(status_code, crc),
            ))
        }
        "Err" => Ok(LogCsvRecord(
            line,
            repetition,
            start,
            end,
            d,
            LogCsvResult::Err(error.as_ref().to_owned()),
        )),
        _ => bail!("invalid entry in 'Ok/Err' column: {ok_err:?}"),
    }
}

/// Iterator to read back a log file written by the LogCsv writer
pub struct LogCsvReader {
    path: Arc<Path>,
    line0: usize,
    reader: csv::Reader<BufReader<File>>,
    stringrecord: csv::StringRecord,
    fields: RefVecBacking<'static, str>,
}

impl LogCsvReader {
    pub fn open(path: Arc<Path>) -> Result<Self> {
        let log_file = BufReader::new(
            File::open(&*path).with_context(|| anyhow!("opening {path:?} for reading"))?,
        );
        let reader = csv::Reader::from_reader(log_file);
        Ok(Self {
            path,
            line0: 0,
            reader,
            stringrecord: csv::StringRecord::new(),
            fields: RefVecBacking::new(),
        })
    }
}

impl Iterator for LogCsvReader {
    type Item = Result<LogCsvRecord>;

    fn next(&mut self) -> Option<Result<LogCsvRecord>> {
        match self
            .reader
            .read_record(&mut self.stringrecord)
            .with_context(|| anyhow!(""))
        {
            Ok(true) => {
                let mut fields = self.fields.borrow_mut();
                for field in &self.stringrecord {
                    fields.push(field);
                }
                let sl = fields.as_slice();
                match sl.try_into() {
                    Ok(arf) => Some(parse_row(arf)),
                    Err(_) => Some(Err(anyhow!(
                        "invalid number of columns: expected {NUM_COLS}, got {} at {:?}:{}",
                        sl.len(),
                        self.path,
                        self.line0 + 1
                    ))),
                }
            }
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// The api-query log file in CSV format
struct LogCsv {
    path: Arc<Path>,
    writer: csv::Writer<BufWriter<File>>,
}

impl LogCsv {
    fn create(path: Arc<Path>) -> Result<Self> {
        let log_file = BufWriter::new(
            File::create(&*path).with_context(|| anyhow!("opening {path:?} for writing"))?,
        );

        let mut writer = csv::Writer::from_writer(log_file);
        writer
            .write_record(HEADER)
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
