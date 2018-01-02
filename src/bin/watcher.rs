#[macro_use]
extern crate error_chain;
extern crate deneb;
#[macro_use]
extern crate log;
extern crate notify;
extern crate rust_sodium;

use log::LevelFilter;

use deneb::be::catalog::MemCatalog;
use deneb::be::populate_with_dir;
use deneb::be::store::MemStore;
use deneb::common::errors::*;
use deneb::common::logging;
use deneb::fe::watch::DirectoryWatcher;
use deneb::fe::watch::params::Params;

fn run() -> Result<()> {
    // Initialize the rust_sodium library (needed to make all its functions thread-safe)
    ensure!(rust_sodium::init(),
            "Could not initialize rust_sodium library. Exiting");

    logging::init(LevelFilter::Trace)
        .chain_err(|| "Could not initialize log4rs")?;
    info!("Deneb - dir watcher!");

    let Params {sync_dir, work_dir, chunk_size} = Params::read();
    info!("Sync dir: {:?}", sync_dir);
    info!("Work dir: {:?}", work_dir);

    // Create an object store
    let mut store = MemStore::new();
    let mut catalog = MemCatalog::new();

    populate_with_dir(&mut catalog, &mut store, sync_dir.as_path(), chunk_size)?;
    info!("Catalog populated with initial contents.");
    catalog.show_stats();

    let mut watcher = DirectoryWatcher::new();
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
