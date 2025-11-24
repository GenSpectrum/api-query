use std::{
    fs::read_to_string,
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use api_query::{
    auto_vec::AutoVec,
    get_terminal_width::get_terminal_width,
    log_csv::{LogCsvReader, LogCsvRecord},
    my_crc::Crc,
    types::{Queries, QueryReference, QueryReferenceWithRepetition},
};
use clap::Parser;
use regex::Regex;

#[derive(clap::Parser, Debug)]
#[clap(next_line_help = true)]
#[clap(set_term_width = get_terminal_width())]
struct Opts {
    /// The subcommand to run. Use `--help` after the sub-command to
    /// get a list of the allowed options there.
    #[clap(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    Debug {
        path: PathBuf,
    },
    Compare {
        /// Ignore queries matching this regex
        #[clap(long)]
        ignore: Option<Regex>,

        /// Ignore queries matching the regex in the file with the
        /// given path (with whitespace trimmed from the end)
        #[clap(long)]
        ignore_from: Option<PathBuf>,

        /// Path to the matching queries file for the given CSV log
        /// files; required if `--ignore` is given
        #[clap(long)]
        queries: Option<PathBuf>,

        /// The first CSV log file to compare
        a: PathBuf,
        /// The second CSV log file to compare
        b: PathBuf,
    },
}

struct Sums {
    path: Arc<Path>,
    sums: AutoVec<Crc>,
    seen: AutoVec<u8>,
    errors: Vec<SumError>,
    successes: usize,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SumError {
    NonMatchingCrc {
        reference: QueryReferenceWithRepetition,
        crc: Crc,
    },
}

impl Sums {
    fn new(path: Arc<Path>) -> Self {
        Self {
            path,
            sums: AutoVec::new(Crc(0)),
            seen: AutoVec::new(0),
            errors: Default::default(),
            successes: Default::default(),
        }
    }

    fn len(&self) -> usize {
        assert_eq!(self.sums.len(), self.seen.len());
        self.sums.len()
    }

    fn add(&mut self, record: &LogCsvRecord) {
        if let Some(crc) = record.crc() {
            let i = record.query_reference().query_index_usize();
            let now_uses = self.seen.saturating_inc(i);
            if now_uses > 1 {
                let first_crc = self.sums.get_copy(i);
                if crc == first_crc {
                    self.successes += 1;
                } else {
                    self.errors.push(SumError::NonMatchingCrc {
                        reference: record.query_reference_with_repetition(),
                        crc,
                    });
                }
            } else {
                self.sums.set(i, crc);
            }
        } else {
            // ignore errors
        }
    }
}

struct QueriesWithIgnore {
    path: Arc<Path>,
    queries: Queries,
    ignore_regex: Regex,
}

impl QueriesWithIgnore {
    fn ignore(&self, reference: QueryReference) -> Result<bool> {
        let query = self
            .queries
            .borrow_queries()
            .get(reference.query_index_usize())
            .ok_or_else(|| {
                anyhow!(
                    "query reference for line {reference} is out of range for file {:?}",
                    self.path
                )
            })?;
        Ok(self.ignore_regex.is_match(query.string))
    }
}

fn sums_from_file(ignore: Option<&QueriesWithIgnore>, path: Arc<Path>) -> Result<Sums> {
    let mut sums = Sums::new(path.clone());
    for record in LogCsvReader::open(path)? {
        let record = record?;
        if let Some(ignore) = ignore {
            if ignore.ignore(record.query_reference())? {
                continue;
            }
        }
        sums.add(&record);
    }
    Ok(sums)
}

fn main() -> Result<()> {
    let Opts { command } = Opts::parse();

    match command {
        Command::Debug { path } => {
            for record in LogCsvReader::open(path.into())? {
                let record = record?;
                dbg!(record);
            }
        }
        Command::Compare {
            a,
            b,
            ignore,
            ignore_from,
            queries,
        } => {
            let ignore_regex = if let Some(ignore) = ignore {
                if ignore_from.is_some() {
                    bail!("please only give one of --ignore or --ignore-path")
                }
                Some(ignore)
            } else if let Some(ignore_from) = ignore_from {
                let string = read_to_string(&ignore_from)
                    .with_context(|| anyhow!("reading ignore file at {ignore_from:?}"))?;
                Some(
                    Regex::from_str(string.trim_end())
                        .with_context(|| anyhow!("parsing regex from file at {ignore_from:?}"))?,
                )
            } else {
                None
            };

            let queries_with_ignore = if let Some(ignore_regex) = ignore_regex {
                if let Some(queries) = queries {
                    let path: Arc<Path> = queries.into();
                    let queries = Queries::from_path(&*path)?;
                    Some(QueriesWithIgnore {
                        path,
                        ignore_regex,
                        queries,
                    })
                } else {
                    bail!("missing --queries option, needed for --ignore")
                }
            } else {
                None
            };
            let a = sums_from_file(queries_with_ignore.as_ref(), a.into())?;
            let b = sums_from_file(queries_with_ignore.as_ref(), b.into())?;
            if a.len() != b.len() {
                bail!(
                    "the logs use differing numbers of query entries: {} vs. {}",
                    a.len(),
                    b.len()
                );
            }
            let mut num_errors: usize = 0;
            println!("query file line\tCRC 1\tCRC 2");
            for i in 0..a.len() {
                let asum = a.sums.get_copy(i);
                let bsum = b.sums.get_copy(i);
                if asum != bsum {
                    let line = i + 1;
                    println!("{line}\t{asum}\t{bsum}");
                    num_errors += 1;
                }
            }
            println!("=> {num_errors} queries gave CRC differences");

            for mut sums in [a, b] {
                if !sums.errors.is_empty() {
                    num_errors += sums.errors.len();
                    println!("Errors in {:?}:", sums.path);
                    sums.errors.sort();
                    println!("query file line\trepetition\nfirst CRC\tsubsequent CRC");
                    for sum_error in &sums.errors {
                        match sum_error {
                            SumError::NonMatchingCrc {
                                reference:
                                    QueryReferenceWithRepetition {
                                        query_reference,
                                        repetition,
                                    },
                                crc,
                            } => {
                                let first_crc =
                                    sums.sums.get_copy(query_reference.query_index_usize());
                                println!("{query_reference}\t{repetition}\t{first_crc}\t{crc}");
                            }
                        }
                    }
                }
            }

            if num_errors > 0 {
                exit(1);
            }
        }
    }

    Ok(())
}
