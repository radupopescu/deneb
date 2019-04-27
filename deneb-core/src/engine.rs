use crossbeam_channel::bounded as channel;
use failure::{Error, ResultExt};
use time::now_utc;

use std::{
    cell::RefCell,
    fs::{create_dir_all, File},
    path::{Path, PathBuf},
    rc::Rc,
    thread::spawn as tspawn,
    time::Duration,
};

use crate::{
    catalog::{open_catalog, Catalog, CatalogType},
    errors::{DenebResult, EngineError},
    manifest::Manifest,
    populate_with_dir,
    store::{open_store, Store, StoreType},
    util::atomic_write,
    workspace::Workspace,
};

mod handle;
mod protocol;
mod requests;
mod timer;

use self::{
    protocol::{HandlerProxy, Request, RequestHandler},
    requests::{
        Commit, CreateDir, CreateFile, GetAttr, Lookup, OpenDir, OpenFile, Ping, ReadData, ReadDir,
        ReleaseDir, ReleaseFile, RemoveDir, Rename, SetAttr, StopEngine, Unlink, WriteData,
    },
    timer::{Resolution, Timer},
};

pub use self::{handle::Handle, requests::RequestId};

/// Start engine with pre-built catalog and store
pub fn start_engine_prebuilt(
    catalog: Box<dyn Catalog>,
    store: Box<dyn Store>,
    cmd_queue_size: usize,
    auto_commit_interval: usize,
) -> DenebResult<Handle> {
    let (cmd_tx, cmd_rx) = channel(cmd_queue_size);
    let (quit_tx, quit_rx) = channel(1);
    let engine_hd = Handle::new(cmd_tx, quit_rx);
    let timer_engine_hd = engine_hd.clone();
    let _ = tspawn(move || {
        let mut engine = Engine {
            workspace: Workspace::new(catalog, Rc::new(RefCell::new(store))),
            stopped: false,
        };
        let timer = if auto_commit_interval > 0 {
            let mut t = Timer::new(Resolution::Second);
            t.schedule(
                Duration::from_secs(auto_commit_interval as u64),
                true,
                move || {
                    let _ = timer_engine_hd.commit();
                },
            );
            Some(t)
        } else {
            None
        };
        info!("Starting engine event loop");
        for request in &cmd_rx {
            request.run_handler(&mut engine);
            if engine.stopped {
                break;
            }
        }
        info!("Engine event loop finished.");
        if let Some(timer) = timer {
            timer.stop();
        }
        quit_tx.send(()).map_err(|_| EngineError::Send).unwrap();
    });

    let _ = engine_hd.ping();

    Ok(engine_hd)
}

/// Start the engine using catalog and store builders
pub fn start_engine(
    catalog_type: CatalogType,
    store_type: StoreType,
    work_dir: &Path,
    sync_dir: Option<PathBuf>,
    chunk_size: usize,
    cmd_queue_size: usize,
    auto_commit_interval: usize,
) -> DenebResult<Handle> {
    let (catalog, store) = init(catalog_type, store_type, work_dir, sync_dir, chunk_size)?;

    start_engine_prebuilt(catalog, store, cmd_queue_size, auto_commit_interval)
}

fn init(
    catalog_type: CatalogType,
    store_type: StoreType,
    work_dir: &Path,
    sync_dir: Option<PathBuf>,
    chunk_size: usize,
) -> DenebResult<(Box<dyn Catalog>, Box<dyn Store>)> {
    // Create an object store
    let mut store = open_store(store_type, work_dir, chunk_size)?;

    let catalog_root = work_dir.to_path_buf().join("scratch");
    create_dir_all(catalog_root.as_path())?;
    let catalog_path = catalog_root.join("current_catalog");
    info!("Catalog path: {:?}", catalog_path);

    let manifest_path = work_dir.to_path_buf().join("manifest");
    info!("Manifest path: {:?}", manifest_path);

    // Create the file metadata catalog and populate it with the contents of "sync_dir"
    if let Some(sync_dir) = sync_dir {
        {
            //use std::ops::DerefMut;
            let mut catalog = open_catalog(catalog_type, catalog_path.as_path(), true)?;
            populate_with_dir(&mut *catalog, &mut *store, sync_dir.as_path(), chunk_size)?;
            info!(
                "Catalog populated with contents of {:?}",
                sync_dir.as_path()
            );
        }

        // Save the generated catalog as a content-addressed chunk in the store.
        let mut f = File::open(catalog_path.as_path())?;
        let chunk_descriptor = store.put_file(&mut f)?;

        // Create and save the repository manifest
        let manifest = Manifest::new(chunk_descriptor.digest, None, now_utc());
        manifest.save(manifest_path.as_path())?;
    }

    // Load the repository manifest
    let manifest = Manifest::load(manifest_path.as_path())?;

    // Get the catalog out of storage and open it
    {
        let root_hash = manifest.root_hash;
        let chunk = store.get_chunk(&root_hash)?;
        atomic_write(catalog_path.as_path(), chunk.get_slice())?;
    }

    let catalog = open_catalog(catalog_type, catalog_path.as_path(), false)?;
    catalog.show_stats();

    Ok((catalog, store))
}

pub(in crate::engine) struct Engine {
    workspace: Workspace,
    stopped: bool,
}

impl Engine {
    fn stop(&mut self) {
        info!("Engine stopping...");
        let _ = self.workspace.commit();
        self.stopped = true;
        info!("Engine stopped.");
    }
}

impl RequestHandler<GetAttr> for Engine {
    fn handle(&mut self, request: &GetAttr) -> DenebResult<<GetAttr as Request>::Reply> {
        self.workspace.get_attr(request.index)
            .context(EngineError::GetAttr(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<SetAttr> for Engine {
    fn handle(&mut self, request: &SetAttr) -> DenebResult<<SetAttr as Request>::Reply> {
        self.workspace.set_attr(request.index, &request.changes)
            .context(EngineError::SetAttr(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<Lookup> for Engine {
    fn handle(&mut self, request: &Lookup) -> DenebResult<<Lookup as Request>::Reply> {
        self.workspace.lookup(request.parent, &request.name)
            .context(EngineError::Lookup(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<OpenDir> for Engine {
    fn handle(&mut self, request: &OpenDir) -> DenebResult<<OpenDir as Request>::Reply> {
        self.workspace.open_dir(request.index)
            .context(EngineError::DirOpen(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReleaseDir> for Engine {
    fn handle(&mut self, request: &ReleaseDir) -> DenebResult<<ReleaseDir as Request>::Reply> {
        self.workspace.release_dir(request.index)
            .context(EngineError::DirClose(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReadDir> for Engine {
    fn handle(&mut self, request: &ReadDir) -> DenebResult<<ReadDir as Request>::Reply> {
        self.workspace.read_dir(request.index)
            .context(EngineError::DirRead(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<OpenFile> for Engine {
    fn handle(&mut self, request: &OpenFile) -> DenebResult<<OpenFile as Request>::Reply> {
        self.workspace.open_file(request.index, request.flags)
            .context(EngineError::FileOpen(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReadData> for Engine {
    fn handle(&mut self, request: &ReadData) -> DenebResult<<ReadData as Request>::Reply> {
        self.workspace.read_data(request.index, request.offset, request.size)
            .context(EngineError::FileRead(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<WriteData> for Engine {
    fn handle(&mut self, request: &WriteData) -> DenebResult<<WriteData as Request>::Reply> {
        self.workspace.write_data(request.index, request.offset, &request.data)
            .context(EngineError::FileWrite(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReleaseFile> for Engine {
    fn handle(&mut self, request: &ReleaseFile) -> DenebResult<<ReleaseFile as Request>::Reply> {
        self.workspace.release_file(request.index)
            .context(EngineError::FileClose(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<CreateFile> for Engine {
    fn handle(&mut self, request: &CreateFile) -> DenebResult<<CreateFile as Request>::Reply> {
        self.workspace.create_file(request.parent, &request.name, request.mode, request.flags)
            .context(EngineError::FileCreate(
                request.parent,
                request.name.clone(),
            ))
            .map_err(Error::from)
    }
}

impl RequestHandler<CreateDir> for Engine {
    fn handle(&mut self, request: &CreateDir) -> DenebResult<<CreateDir as Request>::Reply> {
        self.workspace.create_dir(request.parent, &request.name, request.mode)
            .context(EngineError::DirCreate(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<Unlink> for Engine {
    fn handle(&mut self, request: &Unlink) -> DenebResult<<Unlink as Request>::Reply> {
        self.workspace.remove(request.parent, &request.name)
            .context(EngineError::Unlink(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<RemoveDir> for Engine {
    fn handle(&mut self, request: &RemoveDir) -> DenebResult<<RemoveDir as Request>::Reply> {
        self.workspace.remove(request.parent, &request.name)
            .context(EngineError::RemoveDir(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<Rename> for Engine {
    fn handle(&mut self, request: &Rename) -> DenebResult<<Rename as Request>::Reply> {
        self.workspace.rename(
            request.parent,
            &request.name,
            request.new_parent,
            &request.new_name,
        )
        .context(EngineError::Rename(
            request.parent,
            request.name.clone(),
            request.new_parent,
            request.new_name.clone(),
        ))
        .map_err(Error::from)
    }
}

impl RequestHandler<Commit> for Engine {
    fn handle(&mut self, _request: &Commit) -> DenebResult<String> {
        debug!("Engine received commit request.");
        self.workspace.commit()
            .context(EngineError::Commit)
            .map_err(Error::from)
    }
}

impl RequestHandler<Ping> for Engine {
    fn handle(&mut self, _request: &Ping) -> DenebResult<String> {
        debug!("Engine received ping request.");
        Ok("Pong".to_string())
    }
}

impl RequestHandler<StopEngine> for Engine {
    fn handle(&mut self, _request: &StopEngine) -> DenebResult<()> {
        info!("StopEngine request received.");
        self.stop();
        Ok(())
    }
}
