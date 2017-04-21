extern crate clap;
extern crate deneb;
#[macro_use]
extern crate log;
extern crate notify;

use log::LogLevelFilter;

use deneb::catalog::Catalog;
use deneb::errors::*;
use deneb::logging;

mod watch {
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::{Receiver, channel};
    use deneb::errors::*;

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

    pub struct Params {
        pub sync_dir: PathBuf,
        pub work_dir: PathBuf,
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
                .get_matches();

            let sync_dir = PathBuf::from(matches.value_of("sync_dir")
                .map(|d| d.to_string())
                .ok_or_else(|| ErrorKind::CommandLineParameter("sync_dir missing".to_owned()))?);
            let work_dir = PathBuf::from(matches.value_of("work_dir")
                .map(|d| d.to_string())
                .ok_or_else(|| ErrorKind::CommandLineParameter("work_dir missing".to_owned()))?);

            Ok(Params {
                sync_dir: sync_dir,
                work_dir: work_dir,
            })
        }
    }
}

fn run() -> Result<()> {
    logging::init(LogLevelFilter::Trace).chain_err(|| "Could not initialize log4rs")?;
    info!("Deneb - dir watcher!");

    let watch::Params { sync_dir, work_dir } =
        watch::Params::read().chain_err(|| "Could not read command-line parameters")?;
    info!("Sync dir: {:?}", sync_dir);
    info!("Work dir: {:?}", work_dir);

    let catalog : Catalog = Catalog::from_dir(sync_dir.as_path())?;
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
