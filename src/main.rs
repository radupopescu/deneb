extern crate deneb;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate rust_sodium;

use std::fs::create_dir_all;

use deneb::be::catalog::LmdbCatalog;
use deneb::be::populate_with_dir;
use deneb::be::store::DiskStore;
use deneb::common::errors::*;
use deneb::common::logging;
use deneb::common::util::{block_signals, set_sigint_handler};
use deneb::fe::fuse::{AppParameters, Fs};

fn run() -> Result<()> {
    // Block the signals in SigSet on the current and all future threads. Should be run before
    // spawning any new threads.
    block_signals()?;

    // Initialize the rust_sodium library (needed to make all its functions thread-safe)
    ensure!(rust_sodium::init(),
            "Could not initialize rust_sodium library. Exiting");

    let params = AppParameters::read()
        .chain_err(|| "Could not read command-line parameters")?;

    logging::init(params.log_level)
        .chain_err(|| "Could not initialize log4rs")?;

    info!("Welcome to Deneb!");
    info!("Log level: {}", params.log_level);
    info!("Work dir: {:?}", params.work_dir);
    info!("Mount point: {:?}", params.mount_point);
    info!("Chunk size: {:?}", params.chunk_size);
    info!("Sync dir: {:?}", params.sync_dir);

    // Create an object store
    let mut store = DiskStore::at_dir(params.work_dir.as_path())?;

    let catalog_root = params.work_dir.as_path().to_owned().join("scratch");
    create_dir_all(catalog_root.as_path())?;
    let catalog_path = catalog_root.join("current_catalog");
    info!("Catalog path: {:?}", catalog_path);
    let catalog = match params.sync_dir {
        Some(sync_dir) => {
            let mut catalog = LmdbCatalog::create(catalog_path)?;
            // Create the file metadata catalog and populate it with the contents of "sync_dir"
            populate_with_dir(&mut catalog,
                              &mut store,
                              sync_dir.as_path(),
                              params.chunk_size)?;
            info!("Catalog populated with contents of {:?}",
                  sync_dir.as_path());
            catalog
        }
        None => LmdbCatalog::open(catalog_path)?,
    };
    catalog.show_stats();

    // Create the file system data structure
    let file_system = Fs::new(catalog, store);
    let _session = unsafe { file_system.spawn_mount(&params.mount_point, &[])? };

    // Install a handler for Ctrl-C and wait
    let (tx, rx) = std::sync::mpsc::channel();
    let _th = set_sigint_handler(tx)?;
    let _ = rx.recv();

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
