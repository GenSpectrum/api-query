use std::{
    fs::read_to_string,
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    sync::{mpsc::SendError, Arc},
};

use anyhow::{anyhow, bail, Context, Result};
use api_query::{
    auto_vec::AutoVec,
    get_terminal_width::get_terminal_width,
    log_csv::{LogCsvExtendedFormat, LogCsvReader, LogCsvRecord, LogCsvWriter},
    my_crc::Crc,
    types::{Queries, QueryReference, QueryReferenceWithRepetition},
};
use clap::Parser;
use regex::Regex;
use reqwest::StatusCode;

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

    /// Add another column to a CSV log file, with a copy of the query
    /// string that was used, as per the line column.
    Expand {
        /// Overwrite the output file if it exists
        #[clap(short, long)]
        force: bool,

        /// Path to the matching queries file for the given CSV log
        /// files
        queries: PathBuf,

        /// Path to the existing log file
        input: PathBuf,

        /// Path to where the file with the additional column should
        /// be written
        output: PathBuf,
    },

    /// Compare two api-query CSV log files
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

        /// Show the ignored queries
        #[clap(short, long)]
        verbose: bool,

        /// The first CSV log file to compare
        a: PathBuf,
        /// The second CSV log file to compare
        b: PathBuf,
    },
}

struct Sums {
    path: Arc<Path>,
    sums: AutoVec<(StatusCode, usize, Crc)>,
    seen: AutoVec<u8>,
    errors: Vec<SumError>,
    successes: usize,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SumError {
    NonMatchingCrc {
        reference: QueryReferenceWithRepetition,
        crc: (StatusCode, usize, Crc),
    },
}

impl Sums {
    fn new(path: Arc<Path>) -> Self {
        Self {
            path,
            sums: AutoVec::new((StatusCode::from_u16(200).unwrap(), 13131313131313, Crc(0))),
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
        if let Some(crc) = record.status_length_crc() {
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
    queries: Arc<Queries>,
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

fn sums_from_file(ignore: Option<&QueriesWithIgnore>, path: Arc<Path>) -> Result<(usize, Sums)> {
    let mut sums = Sums::new(path.clone());
    let mut num_ignored = 0;
    for record in LogCsvReader::open(path)? {
        let record = record?;
        if let Some(ignore) = ignore {
            if ignore.ignore(record.query_reference())? {
                num_ignored += 1;
                continue;
            }
        }
        sums.add(&record);
    }
    Ok((num_ignored, sums))
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

        Command::Expand {
            force,
            queries,
            input,
            output,
        } => {
            let input = input.into();
            let output = output.into();
            let queries = Queries::from_path(&queries)?.into();
            let log = LogCsvReader::open(input)?;
            let format = LogCsvExtendedFormat { queries };
            let out = LogCsvWriter::create(output, force, format)?;
            enum E {
                Anyhow(anyhow::Error),
                Sendfail(SendError<LogCsvRecord>),
            }
            match (|| -> Result<(), E> {
                for msg in log {
                    let msg = msg.map_err(E::Anyhow)?;
                    out.send(msg).map_err(E::Sendfail)?;
                }
                Ok(())
            })() {
                Ok(()) => {}
                Err(E::Anyhow(e)) => Err(e)?,
                Err(E::Sendfail(e)) => drop(e),
            }
            out.finish()?;
        }

        Command::Compare {
            a,
            b,
            ignore,
            ignore_from,
            queries,
            verbose,
        } => {
            let ignore_regex =
                if let Some(ignore) = ignore {
                    if ignore_from.is_some() {
                        bail!("please only give one of --ignore or --ignore-path")
                    }
                    Some(ignore)
                } else if let Some(ignore_from) = ignore_from {
                    let string = read_to_string(&ignore_from)
                        .with_context(|| anyhow!("reading ignore file at {ignore_from:?}"))?;
                    let re = string.trim_end();
                    if re.is_empty() {
                        if verbose {
                            eprintln!(
                                "ignoring --ignore-from file {ignore_from:?} since it is empty; \
                                 if you want to match a space, please append `{{1}}`"
                            );
                        }
                        None
                    } else {
                        Some(Regex::from_str(re).with_context(|| {
                            anyhow!("parsing regex from file at {ignore_from:?}")
                        })?)
                    }
                } else {
                    None
                };

            let path_and_queries: Option<(Arc<Path>, Arc<Queries>)> = if let Some(queries) = queries
            {
                let path: Arc<Path> = queries.into();
                let queries = Queries::from_path(&*path)?;
                Some((path, queries.into()))
            } else {
                None
            };

            let queries_with_ignore = if let Some(ignore_regex) = ignore_regex {
                if let Some((path, queries)) = &path_and_queries {
                    let queries_with_ignore = QueriesWithIgnore {
                        path: path.clone(),
                        ignore_regex,
                        queries: queries.clone(),
                    };
                    if verbose {
                        for (i, query) in queries_with_ignore
                            .queries
                            .borrow_queries()
                            .iter()
                            .enumerate()
                        {
                            let reference = QueryReference {
                                query_index: i as u32,
                            };
                            if queries_with_ignore.ignore(reference)? {
                                println!(
                                    "api-query-log: will ignore query from line {reference}: {:?}",
                                    query.string
                                );
                            }
                        }
                    }
                    Some(queries_with_ignore)
                } else {
                    bail!("missing --queries option, needed for --ignore")
                }
            } else {
                None
            };
            let (num_a_original_ignored, a) =
                sums_from_file(queries_with_ignore.as_ref(), a.into())?;
            let (num_b_original_ignored, b) =
                sums_from_file(queries_with_ignore.as_ref(), b.into())?;
            if a.len() != b.len() {
                bail!(
                    "the logs use differing numbers of query entries: {} vs. {}",
                    a.len(),
                    b.len()
                );
            }
            let mut num_errors: usize = 0;
            let mut num_same: usize = 0;
            let mut num_ignored_counted: usize = 0;
            println!(
                "query file line\t\
                 status 1\tlength 1\tCRC 1\t\
                 status 2\tlength 2\tCRC 2\tquery string"
            );
            for i in 0..a.len() {
                match (a.seen.get_copy(i) > 0, b.seen.get_copy(i) > 0) {
                    (false, false) => {
                        num_ignored_counted += 1;
                    }
                    (true, true) => {
                        let alen_and_sum = a.sums.get_copy(i);
                        let blen_and_sum = b.sums.get_copy(i);
                        if alen_and_sum == blen_and_sum {
                            num_same += 1;
                        } else {
                            let line = i + 1;
                            let query_string = if let Some((_, query)) = &path_and_queries {
                                if let Some(query) = query.borrow_queries().get(i) {
                                    &query.string
                                } else {
                                    "<error: line is not in given query file>"
                                }
                            } else {
                                "<error: missing --queries option>"
                            };
                            let (astatus, alen, asum) = alen_and_sum;
                            let (bstatus, blen, bsum) = blen_and_sum;
                            println!(
                                "{line}\t{astatus}\t{alen}\t{asum}\t{bstatus}\t{blen}\t{bsum}\t\
                                 {query_string}"
                            );
                            num_errors += 1;
                        }
                    }
                    (aseen, bseen) => {
                        bail!(
                            "bug?: query line {} was seeen: in a: {aseen}, in b: {bseen}",
                            i + 1
                        )
                    }
                }
            }
            let num_total_queries = if let Some((_path, queries)) = &path_and_queries {
                queries.borrow_queries().len()
            } else {
                // If there's no queries, then we can't get the count
                // from it, but we also don't filter, ergo can rely on
                // the count being correct (no gaps)
                assert_eq!(a.seen.len(), b.seen.len());
                a.seen.len()
            };
            assert!(num_errors + num_same + num_ignored_counted <= num_total_queries);
            let num_ignored_calculated =
                num_total_queries - (num_errors + num_same + num_ignored_counted);
            println!(
                "=> {num_errors} queries gave CRC differences, {num_same} had the same CRC, \
                 {num_ignored_calculated} were ignored \
                 ({num_a_original_ignored} and {num_b_original_ignored} requests)"
            );

            for mut sums in [a, b] {
                if !sums.errors.is_empty() {
                    num_errors += sums.errors.len();
                    println!("Errors in {:?}:", sums.path);
                    sums.errors.sort();
                    println!(
                        "query file line\trepetition\tfirst status\nfirst len\tfirst CRC\t\
                         subsequent status\tsubsequent len\tsubsequent CRC"
                    );
                    for sum_error in &sums.errors {
                        match sum_error {
                            SumError::NonMatchingCrc {
                                reference:
                                    QueryReferenceWithRepetition {
                                        query_reference,
                                        repetition,
                                    },
                                crc: (status, len, crc),
                            } => {
                                let (first_status, first_len, first_crc) =
                                    sums.sums.get_copy(query_reference.query_index_usize());
                                println!(
                                    "{query_reference}\t{repetition}\t{first_status}\t{first_len}\t\
                                     {first_crc}\t{status}\t{len}\t{crc}");
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
