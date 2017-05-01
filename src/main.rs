extern crate deneb;
#[macro_use]
extern crate error_chain;
extern crate fuse;
#[macro_use]
extern crate log;
extern crate rust_sodium;

use deneb::catalog::Catalog;
use deneb::errors::*;
use deneb::fs::Fs;
use deneb::logging;
use deneb::params::AppParameters;
use deneb::store::HashMapStore;

fn run() -> Result<()> {
    // Initialize the rust_sodium library (needed to make all its functions thread-safe)
    ensure!(rust_sodium::init(),
            "Could not initialize rust_sodium library. Exiting");

    let params = AppParameters::read()
        .chain_err(|| "Could not read command-line parameters")?;

    logging::init(params.log_level)
        .chain_err(|| "Could not initialize log4rs")?;

    info!("Welcome to Deneb!");
    info!("Log level: {}", params.log_level);
    info!("Sync dir: {:?}", params.sync_dir);
    info!("Work dir: {:?}", params.work_dir);
    info!("Mount point: {:?}", params.mount_point);

    // Create an object store
    let mut store: HashMapStore = HashMapStore::new();

    // Create the file metadata catalog and populate it with the contents of "sync_dir"
    let catalog: Catalog = Catalog::with_dir(params.sync_dir.as_path(), &mut store)?;
    info!("Catalog populated with initial contents.");
    catalog.show_stats();
    store.show_stats();

    // Create the file system data structure
    let file_system = Fs::new(catalog, store);
    fuse::mount(file_system, &params.mount_point, &[])?;

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
