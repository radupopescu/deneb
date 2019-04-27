use {
    self::{
        protocol::{HandlerProxy, Request, RequestHandler},
        requests::{
            Commit, CreateDir, CreateFile, GetAttr, Lookup, OpenDir, OpenFile, Ping, ReadData,
            ReadDir, ReleaseDir, ReleaseFile, RemoveDir, Rename, SetAttr, StopEngine, Unlink,
            WriteData,
        },
    },
    crate::{
        catalog::CatalogType,
        errors::{DenebResult, EngineError},
        store::StoreType,
        workspace::Workspace,
    },
    crossbeam_channel::bounded as channel,
    failure::{Error, ResultExt},
    log::{debug, info},
    std::{
        path::PathBuf,
        thread::{spawn, JoinHandle},
        time::Duration,
    },
    timer::{Resolution, Timer},
};

pub use self::{handle::Handle, requests::RequestId};

mod handle;
mod protocol;
mod requests;
mod timer;

/// Start engine with pre-built catalog and store
pub fn start_engine(
    catalog_type: CatalogType,
    store_type: StoreType,
    work_dir: PathBuf,
    sync_dir: Option<PathBuf>,
    chunk_size: usize,
    cmd_queue_size: usize,
    auto_commit_interval: usize,
) -> DenebResult<Handle> {
    let (cmd_tx, cmd_rx) = channel(cmd_queue_size);
    let (quit_tx, quit_rx) = channel(1);
    let engine_hd = Handle::new(cmd_tx, quit_rx);
    let timer_engine_hd = engine_hd.clone();
    let _: JoinHandle<DenebResult<()>> = spawn(move || {
        let ws = Workspace::new(catalog_type, store_type, work_dir, sync_dir, chunk_size);
        if ws.is_err() {
            panic!("Could not initialize workspace. Engine will not start.");
        }
        let mut engine = Engine {
            workspace: ws?,
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

        Ok(())
    });

    let _ = engine_hd.ping();

    Ok(engine_hd)
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
        self.workspace
            .get_attr(request.index)
            .context(EngineError::GetAttr(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<SetAttr> for Engine {
    fn handle(&mut self, request: &SetAttr) -> DenebResult<<SetAttr as Request>::Reply> {
        self.workspace
            .set_attr(request.index, &request.changes)
            .context(EngineError::SetAttr(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<Lookup> for Engine {
    fn handle(&mut self, request: &Lookup) -> DenebResult<<Lookup as Request>::Reply> {
        self.workspace
            .lookup(request.parent, &request.name)
            .context(EngineError::Lookup(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<OpenDir> for Engine {
    fn handle(&mut self, request: &OpenDir) -> DenebResult<<OpenDir as Request>::Reply> {
        self.workspace
            .open_dir(request.index)
            .context(EngineError::DirOpen(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReleaseDir> for Engine {
    fn handle(&mut self, request: &ReleaseDir) -> DenebResult<<ReleaseDir as Request>::Reply> {
        self.workspace
            .release_dir(request.index)
            .context(EngineError::DirClose(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReadDir> for Engine {
    fn handle(&mut self, request: &ReadDir) -> DenebResult<<ReadDir as Request>::Reply> {
        self.workspace
            .read_dir(request.index)
            .context(EngineError::DirRead(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<OpenFile> for Engine {
    fn handle(&mut self, request: &OpenFile) -> DenebResult<<OpenFile as Request>::Reply> {
        self.workspace
            .open_file(request.index, request.flags)
            .context(EngineError::FileOpen(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReadData> for Engine {
    fn handle(&mut self, request: &ReadData) -> DenebResult<<ReadData as Request>::Reply> {
        self.workspace
            .read_data(request.index, request.offset, request.size)
            .context(EngineError::FileRead(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<WriteData> for Engine {
    fn handle(&mut self, request: &WriteData) -> DenebResult<<WriteData as Request>::Reply> {
        self.workspace
            .write_data(request.index, request.offset, &request.data)
            .context(EngineError::FileWrite(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReleaseFile> for Engine {
    fn handle(&mut self, request: &ReleaseFile) -> DenebResult<<ReleaseFile as Request>::Reply> {
        self.workspace
            .release_file(request.index)
            .context(EngineError::FileClose(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<CreateFile> for Engine {
    fn handle(&mut self, request: &CreateFile) -> DenebResult<<CreateFile as Request>::Reply> {
        self.workspace
            .create_file(request.parent, &request.name, request.mode, request.flags)
            .context(EngineError::FileCreate(
                request.parent,
                request.name.clone(),
            ))
            .map_err(Error::from)
    }
}

impl RequestHandler<CreateDir> for Engine {
    fn handle(&mut self, request: &CreateDir) -> DenebResult<<CreateDir as Request>::Reply> {
        self.workspace
            .create_dir(request.parent, &request.name, request.mode)
            .context(EngineError::DirCreate(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<Unlink> for Engine {
    fn handle(&mut self, request: &Unlink) -> DenebResult<<Unlink as Request>::Reply> {
        self.workspace
            .remove(request.parent, &request.name)
            .context(EngineError::Unlink(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<RemoveDir> for Engine {
    fn handle(&mut self, request: &RemoveDir) -> DenebResult<<RemoveDir as Request>::Reply> {
        self.workspace
            .remove(request.parent, &request.name)
            .context(EngineError::RemoveDir(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<Rename> for Engine {
    fn handle(&mut self, request: &Rename) -> DenebResult<<Rename as Request>::Reply> {
        self.workspace
            .rename(
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
        self.workspace
            .commit()
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
