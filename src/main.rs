extern crate deneb;
extern crate failure;
#[macro_use]
extern crate log;

extern crate deneb_core;
extern crate deneb_fuse;

use failure::ResultExt;

use deneb_core::{
    catalog::CatalogType, engine::start_engine, errors::DenebResult, store::StoreType,
};
use deneb_fuse::fs::Fs;

use deneb::{
    app::App,
    logging::init_logger,
    util::{block_signals, fork, set_sigint_handler},
};

fn main() -> DenebResult<()> {
    let app = App::init()?;

    // If not instructed to stay in the foreground, do a double-fork
    // and exit in the parent and child processes. Only the grandchild
    // process is allowed to continue
    if !app.settings.foreground {
        if !fork(true) {
            return Ok(());
        }
    }

    // Block the signals in SigSet on the current and all future threads. Should be run before
    // spawning any new threads.
    block_signals().context("Could not block signals in current thread")?;

    // Initialize deneb-core
    deneb_core::init()?;

    init_logger(app.settings.log_level).context("Could not initialize logger")?;

    info!("Welcome to Deneb!");
    info!("Log level: {}", app.settings.log_level);
    info!("Work dir: {:?}", app.directories.workspace);
    info!("Mount point: {:?}", app.directories.mount_point);
    info!("Chunk size: {:?}", app.settings.chunk_size);
    info!("Sync dir: {:?}", app.settings.sync_dir);
    info!("Force unmount: {}", app.settings.force_unmount);

    // Create the file system data structure
    let handle = start_engine(
        CatalogType::Lmdb,
        StoreType::OnDisk,
        &app.directories.workspace,
        app.settings.sync_dir.clone(),
        app.settings.chunk_size,
        1000,
    )?;

    let options = Fs::make_options(&[
        format!("negative_vncache"),
        format!("fsname={}", app.fs_name()),
        format!("volname={}", app.settings.instance_name),
    ]);

    if app.settings.foreground {
        let session = Fs::spawn_mount(&app.directories.mount_point, handle.clone(), &options)?;

        // Install a handler for Ctrl-C and wait
        let (tx, rx) = std::sync::mpsc::channel();
        let _th = set_sigint_handler(tx);
        rx.recv()?;

        info!("Ctrl-C received. Exiting.");

        handle.stop_engine();

        // Force unmount the file system
        if app.settings.force_unmount {
            info!("Force unmounting the file system.");
            session.force_unmount()?;
        }
    } else {
        Fs::mount(&app.directories.mount_point, handle.clone(), &options)?;
        handle.stop_engine();
    }

    Ok(())
}
