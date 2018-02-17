extern crate deneb;
extern crate failure;
#[macro_use]
extern crate log;

extern crate deneb_core;
extern crate deneb_fuse;

use failure::ResultExt;

use deneb_core::errors::{print_error_with_causes, DenebResult};
use deneb_core::catalog::LmdbCatalogBuilder;
use deneb_core::engine::start_engine;
use deneb_core::store::DiskStoreBuilder;
use deneb_fuse::fs::Fs;

use deneb::logging::init_logger;
use deneb::params::AppParameters;
use deneb::util::{block_signals, set_sigint_handler};

fn run() -> DenebResult<()> {
    // Block the signals in SigSet on the current and all future threads. Should be run before
    // spawning any new threads.
    block_signals().context("Could not block signals in current thread")?;

    // Initialize deneb-core
    deneb_core::init()?;

    let params = AppParameters::read();

    init_logger(params.log_level).context("Could not initialize logger")?;

    info!("Welcome to Deneb!");
    info!("Log level: {}", params.log_level);
    info!("Work dir: {:?}", params.work_dir);
    info!("Mount point: {:?}", params.mount_point);
    info!("Chunk size: {:?}", params.chunk_size);
    info!("Sync dir: {:?}", params.sync_dir);
    info!("Force unmount: {}", params.force_unmount);

    // Create the file system data structure
    let cb = LmdbCatalogBuilder;
    let sb = DiskStoreBuilder;
    let handle = start_engine(
        &cb,
        &sb,
        &params.work_dir,
        params.sync_dir,
        params.chunk_size,
        1000,
    )?;
    let file_system = Fs::new(handle);
    let _session = unsafe { file_system.spawn_mount(&params.mount_point, &[])? };

    // Install a handler for Ctrl-C and wait
    let (tx, rx) = std::sync::mpsc::channel();
    let _th = set_sigint_handler(tx);
    rx.recv()?;

    info!("Ctrl-C received. Exiting.");

    Ok(())
}

fn main() {
    if let Err(ref fail) = run() {
        print_error_with_causes(fail);
        error!("Backtrace: {}", fail.backtrace());

        ::std::process::exit(1)
    }
}
