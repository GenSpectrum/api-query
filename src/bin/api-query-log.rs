use std::{
    path::{Path, PathBuf},
    process::exit,
    sync::Arc,
};

use anyhow::{bail, Result};
use api_query::{
    auto_vec::AutoVec,
    get_terminal_width::get_terminal_width,
    log_csv::{LogCsvReader, LogCsvRecord},
    my_crc::Crc,
    types::QueryReferenceWithRepetition,
};
use clap::Parser;

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
    Debug { path: PathBuf },
    Compare { a: PathBuf, b: PathBuf },
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

fn sums_from_file(path: Arc<Path>) -> Result<Sums> {
    let mut sums = Sums::new(path.clone());
    for record in LogCsvReader::open(path)? {
        sums.add(&record?);
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
        Command::Compare { a, b } => {
            let a = sums_from_file(a.into())?;
            let b = sums_from_file(b.into())?;
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
