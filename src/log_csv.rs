use std::{
    borrow::Cow,
    convert::TryInto,
    fs::File,
    io::{BufReader, BufWriter},
    marker::PhantomData,
    path::Path,
    sync::{
        mpsc::{self, SendError},
        Arc,
    },
    thread,
};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::StatusCode;

use crate::{
    cowstr::Cowstr,
    my_crc::Crc,
    time::UnixTimeWrap,
    types::{Queries, QueryReference, QueryReferenceWithRepetition},
    vec_backing::RefVecBacking,
};

/// The result of a query
#[derive(Debug)]
pub enum LogCsvResult {
    Ok(StatusCode, usize, Crc),
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
            LogCsvResult::Ok(_status_code, _length, crc) => Some(*crc),
            LogCsvResult::Err(_) => None,
        }
    }
    /// The response info when there is one (non-error cases).
    pub fn status_length_crc(&self) -> Option<(StatusCode, usize, Crc)> {
        match self.result() {
            LogCsvResult::Ok(status_code, length, crc) => Some((*status_code, *length, *crc)),
            LogCsvResult::Err(_) => None,
        }
    }
}

pub trait Format {
    const NUM_COLS: usize;
    fn header<'t>(&'t self) -> Cow<'t, [&'static str]>;
    fn queries(&self) -> Option<&Queries>;
}

pub struct LogCsvExtendedFormat {
    pub queries: Arc<Queries>,
}

impl Format for LogCsvExtendedFormat {
    const NUM_COLS: usize = LogCsvNormalFormat::NUM_COLS + 1;

    fn header<'t>(&'t self) -> Cow<'t, [&'static str]> {
        let mut v: Vec<_> = LogCsvNormalFormat::HEADER.iter().copied().collect();
        v.push("query string");
        v.into()
    }

    fn queries(&self) -> Option<&Queries> {
        Some(&self.queries)
    }
}

pub struct LogCsvNormalFormat;

impl Format for LogCsvNormalFormat {
    const NUM_COLS: usize = LogCsvNormalFormat::NUM_COLS;

    fn header<'t>(&'t self) -> Cow<'t, [&'static str]> {
        (&Self::HEADER).into()
    }

    fn queries(&self) -> Option<&Queries> {
        None
    }
}

impl LogCsvNormalFormat {
    const NUM_COLS: usize = 10;
    const HEADER: [&str; Self::NUM_COLS] = [
        "line in query file",
        "repetition",
        "start",
        "end",
        "d",
        "Ok/Err",
        "status",
        "length",
        "crc",
        "error",
    ];

    pub fn parse_row(row: &[impl AsRef<str>; Self::NUM_COLS]) -> Result<LogCsvRecord> {
        let [line, repetition, start, end, d, ok_err, status_code, length, crc, error] = row;

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
                // Split "200 OK" into just "200"
                let (status_code, _) = status_code
                    .as_ref()
                    .split_once(' ')
                    .ok_or_else(|| anyhow!("expecting status code number followed by a space"))?;

                let_parse!(status_code ? "HTTP status code");
                let_parse!(length ? "length");
                let_parse!(crc ? "CRC");

                Ok(LogCsvRecord(
                    line,
                    repetition,
                    start,
                    end,
                    d,
                    LogCsvResult::Ok(status_code, length, crc),
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
    // type Format = LogCsvNormalFormat; -- unstable, see inside `next()` instead

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
        type Format = LogCsvNormalFormat;

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
                    Ok(arf) => Some(Format::parse_row(arf)),
                    Err(_) => Some(Err(anyhow!(
                        "invalid number of columns: expected {}, got {} at {:?}:{}",
                        Format::NUM_COLS,
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
struct LogCsv<F: Format> {
    path: Arc<Path>,
    writer: csv::Writer<BufWriter<File>>,
    format: F,
}

impl<F: Format> LogCsv<F> {
    fn create(path: Arc<Path>, overwrite: bool, format: F) -> Result<Self> {
        let mut opt = File::options();
        opt.write(true);
        if overwrite {
            opt.truncate(true);
            opt.create(true);
        } else {
            opt.create_new(true);
        }
        let file = opt
            .open(&*path)
            .with_context(|| anyhow!("opening {path:?} for writing"))?;

        let log_file = BufWriter::new(file);

        let mut writer = csv::Writer::from_writer(log_file);
        writer
            .write_record(&*format.header())
            .with_context(|| anyhow!("writing to CSV log file {path:?}"))?;

        Ok(Self {
            path,
            writer,
            format,
        })
    }

    fn write_row(&mut self, values: LogCsvRecord) -> Result<()> {
        let Self {
            path,
            writer,
            format,
        } = self;

        let LogCsvRecord(a, b, c, d, e, res) = values;
        // lame, wanted to avoid allocations, but there we are (and I
        // don't want to write serde serializers).
        let mut record: [Cowstr; _] = [
            a.to_string().into(),
            b.to_string().into(),
            c.to_string().into(),
            d.to_string().into(),
            e.to_string().into(),
            "".into(), // 5
            "".into(),
            "".into(),
            "".into(),
            "".into(),
            // only used if `queries` was given
            "".into(), // 10
        ];
        match res {
            LogCsvResult::Ok(status_code, length, crc) => {
                record[5] = "Ok".into();
                record[6] = status_code.to_string().into();
                record[7] = length.to_string().into();
                record[8] = crc.to_string().into();
            }
            LogCsvResult::Err(e) => {
                record[5] = "Err".into();
                record[9] = e.into();
            }
        }
        let record_used = if let Some(queries) = format.queries() {
            record[10] = queries.borrow_queries()[a.query_index_usize()]
                .string
                .into();
            &record
        } else {
            &record[..10]
        };

        writer
            .write_record(record_used)
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
pub struct LogCsvWriter<F: Format> {
    thread: thread::JoinHandle<Result<()>>,
    _format: PhantomData<fn() -> F>,
    channel_tx: mpsc::Sender<LogCsvRecord>,
    path: Arc<Path>,
}

impl<F: Format + Send + 'static> LogCsvWriter<F> {
    /// Create a log writer running in a separate thread.
    pub fn create(path: Arc<Path>, overwrite: bool, format: F) -> Result<Self> {
        let mut log_file = LogCsv::create(path.clone(), overwrite, format)?;
        let (channel_tx, channel_rx) = mpsc::channel();
        let thread = thread::spawn(move || -> Result<()> {
            for entry in channel_rx {
                log_file.write_row(entry)?;
            }
            log_file.flush()
        });
        Ok(Self {
            thread,
            _format: PhantomData,
            channel_tx,
            path,
        })
    }

    /// Send a log record to the writer thread / log. Note: be care
    /// ful to run `finish()` at some point after this, to see the
    /// reason why that thread failed! (Consider `LogCsvWriter` to be
    /// a linear type.)
    pub fn send(&self, record: LogCsvRecord) -> Result<(), SendError<LogCsvRecord>> {
        self.channel_tx.send(record)
    }

    /// Finish writing and flushing all buffered messages. Should
    /// always be called, even if `send()` had an error, as only with
    /// this call the reason for errors is revealed.
    pub fn finish(self) -> Result<()> {
        let Self {
            thread,
            _format,
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
