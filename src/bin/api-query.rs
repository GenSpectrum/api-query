use std::{
    collections::{hash_map::Entry, HashMap},
    fs::{create_dir_all, remove_file, rename},
    io::{BufRead, Read},
    ops::{Deref, DerefMut},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use api_query::{
    clone,
    get_terminal_width::get_terminal_width,
    path_util::{add_extension, AppendToPath},
};
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::random;
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
        /// whole list is kept in memory.
        #[clap(long, default_value = "1")]
        repeat: usize,

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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Query {
    /// 0-based position in the queries file
    id: usize,
    /// e.g. line from the queries file, or all of stdin
    string: Arc<str>,
}

impl Query {
    /// The file name is the line number (1-based) of the queries
    /// file, 0-padded for easy sorting.
    pub fn file_name(&self) -> String {
        format!("{:06}", self.id + 1)
    }
}

struct RunQuery {
    endpoint_url: Arc<str>,
    query: Arc<Query>,
}

impl RunQuery {
    /// Returns the HTTP status and the size of the output (even if
    /// the output is dropped)
    async fn run<F: FnMut() -> Client>(
        &self,
        client: PoolGuard<Client, F>,
        output_mode: OutputMode,
    ) -> Result<(StatusCode, usize)> {
        let mut res: Response = client
            .post(&*self.endpoint_url)
            .header("Connection", "keep-alive") // should be default anyway, but silo doesn't do it
            .body((&*self.query.string).to_owned())
            .send()
            .await
            .with_context(|| anyhow!("posting the query {:?}", &*self.query))?;
        let status = res.status();
        let mut outsize = 0;
        if output_mode.is_drop() {
            while let Some(bytes) = res
                .chunk()
                .await
                .with_context(|| anyhow!("reading the result from query {:?}", self.query))?
            {
                outsize += bytes.len();
            }
        } else {
            let (mut out, outpath) = output_mode.output(&self.query.file_name()).await?;
            let mut outsize = 0;
            while let Some(bytes) = res
                .chunk()
                .await
                .with_context(|| anyhow!("reading the result from query {:?}", self.query))?
            {
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
            let mut query = String::new();
            std::io::stdin()
                .read_to_string(&mut query)
                .with_context(|| anyhow!("reading from stdin"))?;
            let rq = RunQuery {
                query: Query {
                    string: query.into(),
                    id: 0,
                }
                .into(),
                endpoint_url,
            };
            let client = client_pool.get_item();
            let (status, _response_len) = rq.run(client, OutputMode::Print).await?;
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
        } => {
            let concurrency: usize = concurrency.unwrap_or(1).max(1).into();
            let output_mode = OutputMode::from_options(outdir, drop_output)?;

            let queries_from_file: Vec<(u64, Arc<Query>)> = std::io::BufReader::new(
                std::fs::File::open(&*queries_path)
                    .with_context(|| anyhow!("opening {queries_path:?} for reading"))?,
            )
            .lines()
            .enumerate()
            .map(|(id, query)| -> Result<_> {
                Ok((
                    random(),
                    Arc::new(Query {
                        id,
                        string: query?.into(),
                    }),
                ))
            })
            .collect::<Result<_>>()?;

            let mut queries = Vec::new();
            for _ in 0..repeat {
                queries.append(&mut queries_from_file.clone());
            }

            if randomize {
                queries.sort();
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

            let mut queries = queries.iter();
            while let Some((_, query)) = queries.next() {
                if verbose {
                    println!("while: {running_tasks} of {concurrency}");
                }
                if running_tasks >= concurrency {
                    await_one_task(&mut tasks, &mut running_tasks).await?;
                }
                let task = tokio::spawn({
                    clone!(query, endpoint_url, client_pool, output_mode,);
                    async move {
                        let rq = RunQuery {
                            query,
                            endpoint_url,
                        };
                        let client = client_pool.get_item();
                        let status = rq.run(client, output_mode).await?;
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
