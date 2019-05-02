use {
    crossbeam_channel::bounded as channel,
    deneb::{
        app::App,
        logging::init_logger,
        talk::{listen, Command},
        util::{block_signals, fork, set_signal_handler},
    },
    deneb_core::{
        catalog::CatalogType, engine::start_engine, errors::DenebResult, store::StoreType,
    },
    deneb_fuse::fs::Fs,
    failure::ResultExt,
    log::info,
};

fn main() -> DenebResult<()> {
    let app = App::init()?;

    // If not instructed to stay in the foreground, do a double-fork
    // and exit in the parent and child processes. Only the grandchild
    // process is allowed to continue
    if !app.settings.foreground && !fork(true) {
        return Ok(());
    }

    // Block the signals in SigSet on the current and all future threads. Should be run before
    // spawning any new threads.
    block_signals().context("Could not block signals in current thread")?;

    // Initialize deneb-core
    deneb_core::init()?;

    init_logger(
        app.settings.log_level,
        app.settings.foreground,
        &app.directories.log,
    )
    .context("Could not initialize logger")?;

    info!("Welcome to Deneb!");
    app.print_settings();

    // Create the file system data structure
    let handle = start_engine(
        CatalogType::Lmdb,
        StoreType::OnDisk,
        app.directories.workspace.clone(),
        app.settings.sync_dir.clone(),
        app.settings.chunk_size,
        1000,
        app.settings.auto_commit_interval,
    )?;

    // Start a listener for commands received from deneb-cli
    let handle2 = handle.clone();
    listen(
        app.directories.workspace.join("cmd.sock"),
        move |cmd| match cmd {
            Command::Status => Ok("".to_string()),
            Command::Ping => handle2.ping(),
            Command::Commit => handle2.commit(),
        },
    )?;

    let options = Fs::make_options(&[
        "negative_vncache".to_string(),
        format!("fsname={}", app.fs_name()),
        format!("volname={}", app.settings.instance_name),
    ]);

    if app.settings.foreground {
        let session = Fs::spawn_mount(&app.directories.mount_point, handle.clone(), &options)?;

        // Install a signal handler for SIGINT, SIGHUP and SIGTERM, and wait
        let (tx, rx) = channel(1);
        let _th = set_signal_handler(tx);
        rx.recv()?;

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
