extern crate deneb;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate rust_sodium;
extern crate time;

use deneb::be::catalog::LmdbCatalogBuilder;
use deneb::be::engine::Engine;
use deneb::be::store::DiskStoreBuilder;
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

    // Create the file system data structure
    let cb = LmdbCatalogBuilder;
    let sb = DiskStoreBuilder;
    let engine = Engine::new(cb,
                             sb,
                             params.work_dir,
                             params.sync_dir,
                             params.chunk_size,
                             1000)?;
    let file_system = Fs::new(engine.handle(), engine.handle());
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
