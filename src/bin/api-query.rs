use std::{
    collections::{hash_map::Entry, HashMap},
    convert::TryFrom,
    fs::{create_dir_all, remove_file, rename},
    io::Read,
    ops::{Deref, DerefMut, Range},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use api_query::{
    clone,
    get_terminal_width::get_terminal_width,
    path_util::{add_extension, AppendToPath},
};
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;
use reqwest::{Client, Response, StatusCode};
use tokio::{
    self,
    fs::File,
    io::{stdout, AsyncWrite, AsyncWriteExt},
    task::JoinHandle,
};

fn getenv(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(s) => Ok(Some(s)),
        Err(e) => match e {
            std::env::VarError::NotPresent => Ok(None),
            std::env::VarError::NotUnicode(_) => Err(e)?,
        },
    }
}

fn check_status(s: StatusCode) -> Result<()> {
    if s != 200 {
        bail!("status code was not success: {s}")
    }
    Ok(())
}

#[derive(clap::Parser, Debug)]
#[clap(next_line_help = true)]
#[clap(set_term_width = get_terminal_width())]
struct Opts {
    /// The PORT env var can also be set to modify the default url
    #[clap(long)]
    url: Option<String>,

    /// Use the default URL but override the port. The PORT env var
    /// can also be set, but is overridden by this option.
    #[clap(long)]
    port: Option<u16>,

    /// The subcommand to run. Use `--help` after the sub-command to
    /// get a list of the allowed options there.
    #[clap(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Help showing default URL
    Defaults,
    /// Print version
    Version,
    /// Read stdin and send that
    Stdin,
    /// Iterate over the lines of a file, each representing a query
    Iter {
        #[clap(short, long)]
        verbose: bool,

        /// How many requests to run concurrently (default: 1)
        #[clap(short, long)]
        concurrency: Option<u16>,

        /// How many times to repeat the queries from the file
        /// (default: 1). This is done before randomization, i.e. the
        /// whole list is kept in memory, but only 8 additional bytes
        /// are used per repetition.
        #[clap(long, default_value = "1")]
        repeat: usize,

        /// Do not run the queries, just show the (possibly
        /// randomized) list of queries to be issued.
        #[clap(long)]
        dry_run: bool,

        /// Do not run the queries, just sleep for 10 seconds after
        /// producing the repeated query set, to allow to check the
        /// memory use.
        #[clap(long)]
        bench_memory: bool,

        /// Whether to randomize the order of the requests (default: no)
        #[clap(short, long)]
        randomize: bool,

        /// Path to a directory where each output should be written to as a file
        #[clap(short, long)]
        outdir: Option<PathBuf>,

        /// Whether to drop the output (default: print to stdout, or
        /// if --outdir is given, write there). Overrides --outdir.
        #[clap(short = 'd', long = "drop")]
        drop_output: bool,

        /// Path to a file with one query per line
        queries_path: PathBuf,
    },
}

#[derive(Clone)]
enum OutputMode {
    Print,
    Outdir(PathBuf),
    Drop,
}

impl OutputMode {
    fn from_options(outdir: Option<PathBuf>, drop_output: bool) -> Result<Self> {
        if drop_output {
            Ok(Self::Drop)
        } else if let Some(outdir) = outdir {
            create_dir_all(&outdir)
                .with_context(|| anyhow!("can't create dir or its parents: {outdir:?}"))?;
            Ok(Self::Outdir(outdir))
        } else {
            Ok(Self::Print)
        }
    }

    fn is_stdout(&self) -> bool {
        match self {
            OutputMode::Print => true,
            OutputMode::Outdir(_) => false,
            OutputMode::Drop => false,
        }
    }

    fn is_drop(&self) -> bool {
        match self {
            OutputMode::Print => false,
            OutputMode::Outdir(_) => false,
            OutputMode::Drop => true,
        }
    }

    /// Returns filehandle and, if applicable, path to the output file.
    async fn output(
        &self,
        file_name: &str,
    ) -> Result<(Pin<Box<dyn AsyncWrite + Send>>, Option<PathBuf>)> {
        match self {
            OutputMode::Print => Ok((Box::pin(stdout()), None)),
            OutputMode::Outdir(path_buf) => {
                let path = path_buf.append(file_name);
                Ok((
                    Box::pin(
                        File::options()
                            .create(true)
                            .truncate(true)
                            .write(true)
                            .open(&path)
                            .await?,
                    ),
                    Some(path),
                ))
            }
            OutputMode::Drop => Ok((Box::pin(stdout()), None)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Query<'s> {
    /// e.g. line from the queries file, or all of stdin
    string: &'s str,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct QueryInstance {
    query_index: u32,
    /// 0-based, i.e. 1 is the first *repetition*
    repetition: u32,
}

#[test]
fn t_sizes() {
    assert_eq!(size_of::<Query>(), 16);
    assert_eq!(size_of::<[Query; 2]>(), 32);
    assert_eq!(size_of::<QueryInstance>(), 8);
    assert_eq!(size_of::<[QueryInstance; 2]>(), 16);
}

#[ouroboros::self_referencing]
struct Queries {
    queries_string: String,
    #[borrows(queries_string)]
    #[covariant]
    queries: Vec<Query<'this>>,
}

impl Queries {
    fn _new(queries_string: String, split: bool) -> Result<Self> {
        Self::try_new(queries_string, |queries_string| -> Result<_> {
            let queries: Vec<Query> = if split {
                queries_string
                    .split('\n')
                    .map(|string| Query { string })
                    .collect()
            } else {
                vec![Query {
                    string: queries_string,
                }]
            };
            (|| -> Option<_> {
                let maxline: usize = queries.len().checked_add(1)?;
                let _maxline: u32 = u32::try_from(maxline).ok()?;
                Some(())
            })()
            .ok_or_else(|| anyhow!(">= u32 lines in file"))?;
            Ok(queries)
        })
    }

    pub fn from_lines_string(queries_string: String) -> Result<Self> {
        Self::_new(queries_string, true)
    }

    pub fn from_single_query(queries_string: String) -> Result<Self> {
        Self::_new(queries_string, false)
    }

    fn get_query(&self, i: u32) -> Query {
        self.borrow_queries()[usize::try_from(i).expect("correct index generation")].clone()
    }

    fn query_index_range(&self) -> Range<usize> {
        0..self.borrow_queries().len()
    }
}

impl QueryInstance {
    pub fn query<'q>(&self, queries: &'q Queries) -> Query<'q> {
        queries.get_query(self.query_index)
    }

    /// The file name is the line number (1-based) of the queries
    /// file, 0-padded for easy sorting, and the repetition count
    /// (0-based) for that query if a non-1 repetition count was
    /// requested.
    pub fn output_file_name(&self, show_repetition: bool) -> String {
        let line = u64::from(self.query_index) + 1;
        if show_repetition {
            let repetition = self.repetition;
            format!("{line:06}-{repetition:06}")
        } else {
            format!("{line:06}")
        }
    }
}

struct RunQuery {
    endpoint_url: Arc<str>,
    query_instance: QueryInstance,
}

impl RunQuery {
    /// Returns the HTTP status and the size of the output (even if
    /// the output is dropped)
    async fn run<F: FnMut() -> Client>(
        &self,
        client: PoolGuard<Client, F>,
        output_mode: OutputMode,
        show_repetition: bool,
        queries: &Queries,
    ) -> Result<(StatusCode, usize)> {
        let mut res: Response = client
            .post(&*self.endpoint_url)
            .header("Connection", "keep-alive") // should be default anyway, but silo doesn't do it
            .body(self.query_instance.query(queries).string.to_owned())
            .send()
            .await
            .with_context(|| {
                anyhow!(
                    "posting the query {:?}",
                    self.query_instance.query(queries).string
                )
            })?;
        let status = res.status();
        let mut outsize = 0;
        if output_mode.is_drop() {
            while let Some(bytes) = res.chunk().await.with_context(|| {
                anyhow!(
                    "reading the result from query {:?}",
                    self.query_instance.query(queries).string
                )
            })? {
                outsize += bytes.len();
            }
        } else {
            let (mut out, outpath) = output_mode
                .output(&self.query_instance.output_file_name(show_repetition))
                .await?;
            let mut outsize = 0;
            while let Some(bytes) = res.chunk().await.with_context(|| {
                anyhow!(
                    "reading the result from query {:?}",
                    self.query_instance.query(queries).string
                )
            })? {
                out.write_all(&bytes)
                    .await
                    .with_context(|| anyhow!("writing to stdout"))?;
                outsize += bytes.len();
            }
            if status != 200 && output_mode.is_stdout() {
                out.write_all(b"\n")
                    .await
                    .with_context(|| anyhow!("writing to stdout"))?;
            }
            out.flush().await?;
            if let Some(outpath) = outpath {
                if outsize == 0 && status == 200 {
                    remove_file(&outpath)
                        .with_context(|| anyhow!("removing output file {outpath:?}"))?
                } else {
                    let mut with_extension = outpath.clone();
                    add_extension(&mut with_extension, format!("{status}"));
                    rename(&outpath, &with_extension)
                        .with_context(|| anyhow!("renaming {outpath:?} to {with_extension:?}"))?;
                }
            }
        }
        Ok((status, outsize))
    }
}

struct PoolInner<T, F: FnMut() -> T> {
    items: Vec<T>,
    new_item: F,
}

struct Pool<T, F: FnMut() -> T>(std::sync::Mutex<PoolInner<T, F>>);

struct PoolGuard<T, F: FnMut() -> T> {
    pool: Arc<Pool<T, F>>,
    item: Option<T>,
}

impl<T, F: FnMut() -> T> Drop for PoolGuard<T, F> {
    fn drop(&mut self) {
        self.pool
            .enqueue(self.item.take().expect("not dropped yet"));
    }
}

impl<T, F: FnMut() -> T> Deref for PoolGuard<T, F> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item.as_ref().expect("not dropped")
    }
}

impl<T, F: FnMut() -> T> DerefMut for PoolGuard<T, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.item.as_mut().expect("not dropped")
    }
}

impl<T, F: FnMut() -> T> Pool<T, F> {
    pub fn new(new_item: F) -> Arc<Self> {
        Self(std::sync::Mutex::new(PoolInner {
            items: Vec::new(),
            new_item,
        }))
        .into()
    }

    pub fn get_item(self: &Arc<Self>) -> PoolGuard<T, F> {
        let item: T = {
            let mut inner = (**self).0.lock().expect("not abandoned");
            inner.items.pop().unwrap_or_else(|| (inner.new_item)())
        };
        PoolGuard {
            pool: self.clone(),
            item: Some(item),
        }
    }

    pub fn enqueue(self: &Arc<Self>, item: T) {
        let mut inner = (**self).0.lock().expect("not abandoned");
        inner.items.push(item);
    }
}

fn default_url(port: Option<u16>) -> Result<String> {
    let port: u16 = if let Some(port) = port {
        port
    } else {
        getenv("PORT")?
            .map(|portstr| {
                portstr
                    .parse()
                    .with_context(|| anyhow!("parsing port string {portstr:?} from PORT env var"))
            })
            .unwrap_or(Ok(8081))?
    };
    Ok(format!("http://localhost:{port}/query").into())
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    let endpoint_url: Arc<str> = if let Some(url) = &opts.url {
        url.as_str().into()
    } else {
        default_url(opts.port)?.into()
    };

    let client_pool: Arc<Pool<Client, _>> = Pool::new(|| Client::new());

    match opts.command {
        Command::Defaults => {
            println!("Default url: {}", default_url(None)?);
        }

        Command::Version => bail!("Not currently implemented"),

        Command::Stdin => {
            let mut query_string = String::new();
            std::io::stdin()
                .read_to_string(&mut query_string)
                .with_context(|| anyhow!("reading from stdin"))?;
            let queries = Queries::from_single_query(query_string)?;
            let rq = RunQuery {
                query_instance: QueryInstance {
                    query_index: 0,
                    repetition: 0,
                },
                endpoint_url,
            };
            let client = client_pool.get_item();
            let (status, _response_len) =
                rq.run(client, OutputMode::Print, false, &queries).await?;
            check_status(status)?;
        }

        Command::Iter {
            concurrency,
            randomize,
            queries_path,
            outdir,
            drop_output,
            verbose,
            repeat,
            dry_run,
            bench_memory,
        } => {
            let concurrency: usize = concurrency.unwrap_or(1).max(1).into();
            let output_mode = OutputMode::from_options(outdir, drop_output)?;

            let show_repetition = repeat != 1;

            let _queries = Box::leak(Box::new(Queries::from_lines_string(
                std::fs::read_to_string(&*queries_path)
                    .with_context(|| anyhow!("reading {queries_path:?}"))?,
            )?));
            let queries = &*_queries;

            let query_instances = {
                let mut query_instances: Vec<QueryInstance> = Vec::new();
                for _ in 0..repeat {
                    for query_index in queries.query_index_range() {
                        query_instances.push(QueryInstance {
                            repetition: 0,
                            query_index: query_index as u32,
                        });
                    }
                }

                let mut rng = rand::thread_rng();
                if randomize {
                    query_instances.shuffle(&mut rng);
                }

                // line0 -> seen, for repetition state during fixup
                // after randomization
                let mut query_counters: Vec<u32> = vec![0].repeat(queries.borrow_queries().len());

                for QueryInstance {
                    query_index,
                    repetition,
                } in &mut query_instances
                {
                    let i = *query_index as usize;
                    *repetition = query_counters[i];
                    query_counters[i] += 1;
                }

                query_instances
            };

            if dry_run {
                for query_instance in query_instances {
                    println!(
                        "{query_instance:?}: {}",
                        query_instance.query(&queries).string
                    );
                }
                return Ok(());
            }

            if bench_memory {
                thread::sleep(Duration::from_secs(10));
                return Ok(());
            }

            struct TaskResult((StatusCode, usize));

            let mut running_tasks = 0;
            // Hard errors
            let mut errors = Vec::new();
            // Soft errors
            let mut status_tally = HashMap::<StatusCode, usize>::new();

            let mut tasks =
                FuturesUnordered::<JoinHandle<Result<TaskResult, anyhow::Error>>>::new();

            let mut await_one_task =
                async |tasks: &mut FuturesUnordered<_>, running_tasks: &mut usize| -> Result<()> {
                    if verbose {
                        println!("await_one_task: {running_tasks}");
                    }
                    let result = tasks
                        .next()
                        .await
                        .ok_or_else(|| anyhow!("no task left, BUG"))?;
                    (*running_tasks) -= 1;
                    match result {
                        Ok(Ok(TaskResult((status, _response_len)))) => {
                            match status_tally.entry(status) {
                                Entry::Occupied(mut occupied_entry) => {
                                    (*occupied_entry.get_mut()) += 1;
                                }
                                Entry::Vacant(vacant_entry) => {
                                    vacant_entry.insert(1);
                                }
                            }
                        }
                        Ok(Err(e)) => errors.push(e),
                        Err(join_error) => bail!("Task panicked: {join_error}"),
                    }

                    if errors.len() > 5 {
                        bail!(
                            "too many errors (besides {:?} ~successes): {errors:?}",
                            status_tally
                        )
                    }
                    Ok(())
                };

            let mut query_instances = query_instances.iter();
            while let Some(query_instance) = query_instances.next() {
                if verbose {
                    println!("while: {running_tasks} of {concurrency}");
                }
                if running_tasks >= concurrency {
                    await_one_task(&mut tasks, &mut running_tasks).await?;
                }
                let task = tokio::spawn({
                    clone!(query_instance, endpoint_url, client_pool, output_mode,);
                    async move {
                        let rq = RunQuery {
                            query_instance,
                            endpoint_url,
                        };
                        let client = client_pool.get_item();
                        let status = rq
                            .run(client, output_mode, show_repetition, queries)
                            .await?;
                        Ok(TaskResult(status))
                    }
                });
                running_tasks += 1;
                tasks.push(task);
            }

            while running_tasks > 0 {
                await_one_task(&mut tasks, &mut running_tasks).await?;
            }
            println!(" ====>  {status_tally:?} ~successes, and errors: {errors:?}");
        }
    }

    Ok(())
}
