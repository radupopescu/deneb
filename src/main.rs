extern crate deneb;
extern crate failure;
#[macro_use]
extern crate log;

extern crate deneb_core;
extern crate deneb_fuse;

use failure::ResultExt;

use std::ffi::OsStr;

use deneb_core::{
    catalog::LmdbCatalogBuilder, engine::start_engine, errors::DenebResult, store::DiskStoreBuilder,
};
use deneb_fuse::fs::Fs;

use deneb::{
    logging::init_logger, params::AppParameters, util::{block_signals, set_sigint_handler},
};

fn main() -> DenebResult<()> {
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

    let options = ["-o", "negative_vncache"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();
    let session = Fs::mount(&params.mount_point, handle, &options)?;

    // Install a handler for Ctrl-C and wait
    let (tx, rx) = std::sync::mpsc::channel();
    let _th = set_sigint_handler(tx);
    rx.recv()?;

    info!("Ctrl-C received. Exiting.");

    // Force unmount the file system
    if params.force_unmount {
        info!("Force unmounting the file system.");
        session.force_unmount()?;
    }

    Ok(())
}
