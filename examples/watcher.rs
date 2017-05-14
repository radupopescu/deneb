extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate deneb;
#[macro_use]
extern crate log;
extern crate notify;
extern crate rust_sodium;

use log::LogLevelFilter;

use deneb::be::catalog::{HashMapCatalog, populate_with_dir};
use deneb::be::store::HashMapStore;
use deneb::common::errors::*;
use deneb::common::logging;

mod watch {
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::{Receiver, channel};
    use deneb::common::errors::*;

    use clap::{App, Arg};
    use notify::{Watcher, RecommendedWatcher, RecursiveMode, RawEvent, raw_watcher};

    pub struct DirectoryWatcher {
        receiver: Receiver<RawEvent>,
        watcher: RecommendedWatcher,
    }

    impl DirectoryWatcher {
        pub fn new() -> DirectoryWatcher {
            // Create a channel to receive the events.
            let (tx, rx) = channel();

            DirectoryWatcher {
                receiver: rx,
                watcher: raw_watcher(tx).unwrap(),
            }
        }

        pub fn watch_path(&mut self, path: &Path) -> ::std::result::Result<(), ::notify::Error> {
            // Add a path to be watched. All files and directories at that path and
            // below will be monitored for changes.
            self.watcher.watch(path, RecursiveMode::Recursive)
        }

        pub fn run(&self) {
            loop {
                match self.receiver.recv() {
                    Ok(event) => info!("{:?}", event),
                    Err(e) => info!("watch error: {:?}", e),
                }
            }
        }
    }

    const DEFAULT_CHUNK_SIZE: u64 = 4194304; // 4MB default

    pub struct Params {
        pub sync_dir: PathBuf,
        pub work_dir: PathBuf,
        pub chunk_size: u64,
    }

    impl Params {
        pub fn read() -> Result<Params> {
            let matches = App::new("Deneb")
                .version("0.1.0")
                .author("Radu Popescu <mail@radupopescu.net>")
                .about("Flew into the light of Deneb")
                .arg(Arg::with_name("sync_dir")
                         .short("s")
                         .long("sync_dir")
                         .takes_value(true)
                         .value_name("SYNC_DIR")
                         .required(true)
                         .help("Synced directory"))
                .arg(Arg::with_name("work_dir")
                         .short("w")
                         .long("work_dir")
                         .takes_value(true)
                         .value_name("WORK_DIR")
                         .required(true)
                         .help("Work (scratch) directory"))
                .arg(Arg::with_name("chunk_size")
                 .long("chunk_size")
                 .takes_value(true)
                 .value_name("CHUNK_SIZE")
                 .required(false)
                 .default_value("DEFAULT")//DEFAULT_CHUNK_SIZE) // default 4MB chunks
                 .help("Chunk size used for storing files"))
                .get_matches();

            let sync_dir = PathBuf::from(matches.value_of("sync_dir")
                .map(|d| d.to_string())
                .ok_or_else(|| ErrorKind::CommandLineParameter("sync_dir missing".to_owned()))?);
            let work_dir = PathBuf::from(matches.value_of("work_dir")
                .map(|d| d.to_string())
                .ok_or_else(|| ErrorKind::CommandLineParameter("work_dir missing".to_owned()))?);
            let chunk_size = match matches.value_of("chunk_size") {
                Some("DEFAULT") | None => DEFAULT_CHUNK_SIZE,
                Some(chunk_size) => {
                    match u64::from_str_radix(chunk_size, 10) {
                        Ok(size) => size,
                        _ => DEFAULT_CHUNK_SIZE,
                    }
                }
            };


            Ok(Params {
                sync_dir: sync_dir,
                work_dir: work_dir,
                chunk_size: chunk_size,
               })
        }
    }
}

fn run() -> Result<()> {
    // Initialize the rust_sodium library (needed to make all its functions thread-safe)
    ensure!(rust_sodium::init(),
            "Could not initialize rust_sodium library. Exiting");

    logging::init(LogLevelFilter::Trace)
        .chain_err(|| "Could not initialize log4rs")?;
    info!("Deneb - dir watcher!");

    let watch::Params { sync_dir, work_dir, chunk_size } =
        watch::Params::read()
            .chain_err(|| "Could not read command-line parameters")?;
    info!("Sync dir: {:?}", sync_dir);
    info!("Work dir: {:?}", work_dir);

    // Create an object store
    let mut store = HashMapStore::new();
    let mut catalog = HashMapCatalog::new();

    populate_with_dir(&mut catalog, &mut store, sync_dir.as_path(), chunk_size)?;
    info!("Catalog populated with initial contents.");
    catalog.show_stats();

    let mut watcher = watch::DirectoryWatcher::new();
    let _ = watcher.watch_path(sync_dir.as_path());
    watcher.run();

    Ok(())
}

fn main() {
    if let Err(ref e) = run() {
        error!("error: {}", e);

        for e in e.iter().skip(1) {
            error!("caused by: {}", e);
        }

        if let Some(bt) = e.backtrace() {
            error!("Backtrace: {:?}", bt);
        }

        ::std::process::exit(1)
    }
}
