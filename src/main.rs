extern crate deneb;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
extern crate rust_sodium;
extern crate time;

use failure::ResultExt;

use deneb::be::catalog::LmdbCatalogBuilder;
use deneb::be::engine::Engine;
use deneb::be::store::DiskStoreBuilder;
use deneb::common::{AppParameters, block_signals, init_logger, set_sigint_handler};
use deneb::common::errors::DenebResult;
use deneb::fe::fuse::Fs;

fn run() -> DenebResult<()> {
    // Block the signals in SigSet on the current and all future threads. Should be run before
    // spawning any new threads.
    block_signals().context("Could not block signals in current thread")?;

    // Initialize the rust_sodium library (needed to make all its functions thread-safe)
    ensure!(rust_sodium::init(),
            "Could not initialize rust_sodium library. Exiting");

    let params = AppParameters::read();

    init_logger(params.log_level).context("Could not initialize logger")?;

    info!("Welcome to Deneb!");
    info!("Log level: {}", params.log_level);
    info!("Work dir: {:?}", params.work_dir);
    info!("Mount point: {:?}", params.mount_point);
    info!("Chunk size: {:?}", params.chunk_size);
    info!("Sync dir: {:?}", params.sync_dir);

    // Create the file system data structure
    let cb = LmdbCatalogBuilder;
    let sb = DiskStoreBuilder;
    let engine = Engine::new(&cb,
                             &sb,
                             &params.work_dir,
                             params.sync_dir,
                             params.chunk_size,
                             1000)?;
    let file_system = Fs::new(engine.handle(), engine.handle());
    let _session = unsafe { file_system.spawn_mount(&params.mount_point, &[])? };

    // Install a handler for Ctrl-C and wait
    let (tx, rx) = std::sync::mpsc::channel();
    let _th = set_sigint_handler(tx);
    let _ = rx.recv();

    Ok(())
}

fn main() {
    if let Err(ref fail) = run() {
        error!("Error: {}", fail);

        {
            let mut fail = fail.cause();
            error!("caused by: {}", fail);
            while let Some(cause) = fail.cause() {
                error!("caused by: {}", cause);
                fail = cause;
            }
        }

        error!("Backtrace: {}", fail.backtrace());

        ::std::process::exit(1)
    }
}
