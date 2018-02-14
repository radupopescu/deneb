use futures::Stream;
use futures::sync::mpsc::channel as future_channel;
use nix::libc::{O_RDWR, O_WRONLY};
use time::now_utc;
use tokio_core::reactor::Core;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::thread::spawn as tspawn;

use catalog::{Catalog, CatalogBuilder};
use file_workspace::FileWorkspace;
use inode::FileType;
use manifest::Manifest;
use populate_with_dir;
use store::{Store, StoreBuilder};
use errors::{DenebResult, EngineError};
use util::atomic_write;

mod protocol;
mod handle;

use self::protocol::{Reply, ReplyChannel, Request};

pub use self::protocol::RequestId;
pub use self::handle::Handle;

/// Start engine with pre-built catalog and store
pub fn start_engine_prebuilt<C, S>(catalog: C, store: S, queue_size: usize) -> DenebResult<Handle>
where
    C: Catalog + Send + 'static,
    S: Store + Send + 'static,
{
    let (tx, rx) = future_channel(queue_size);
    let engine_handle = Handle::new(tx);
    let _ = tspawn(|| {
        if let Ok(mut core) = Core::new() {
            let mut engine = Engine {
                catalog,
                store: Rc::new(RefCell::new(store)),
                open_dirs: HashMap::new(),
                file_workspaces: HashMap::new(),
            };
            let handler = rx.for_each(move |(event, tx)| {
                engine.handle_request(event, &tx);
                Ok(())
            });

            let _ = core.run(handler);
        }
    });

    Ok(engine_handle)
}

/// Start the engine using catalog and store builders
pub fn start_engine<CB, SB>(
    catalog_builder: &CB,
    store_builder: &SB,
    work_dir: &Path,
    sync_dir: Option<PathBuf>,
    chunk_size: usize,
    queue_size: usize,
) -> DenebResult<Handle>
where
    CB: CatalogBuilder,
    <CB as CatalogBuilder>::Catalog: Send + 'static,
    SB: StoreBuilder,
    <SB as StoreBuilder>::Store: Send + 'static,
{
    let (catalog, store) = init(
        catalog_builder,
        store_builder,
        work_dir,
        sync_dir,
        chunk_size,
    )?;

    start_engine_prebuilt(catalog, store, queue_size)
}

fn init<CB, SB>(
    catalog_builder: &CB,
    store_builder: &SB,
    work_dir: &Path,
    sync_dir: Option<PathBuf>,
    chunk_size: usize,
) -> DenebResult<(CB::Catalog, SB::Store)>
where
    CB: CatalogBuilder,
    SB: StoreBuilder,
{
    // Create an object store
    let mut store = store_builder.at_dir(work_dir, chunk_size)?;

    let catalog_root = work_dir.to_path_buf().join("scratch");
    create_dir_all(catalog_root.as_path())?;
    let catalog_path = catalog_root.join("current_catalog");
    info!("Catalog path: {:?}", catalog_path);

    let manifest_path = work_dir.to_path_buf().join("manifest");
    info!("Manifest path: {:?}", manifest_path);

    // Create the file metadata catalog and populate it with the contents of "sync_dir"
    if let Some(sync_dir) = sync_dir {
        {
            let mut catalog = catalog_builder.create(catalog_path.as_path())?;
            populate_with_dir(&mut catalog, &mut store, sync_dir.as_path(), chunk_size)?;
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
        let buffer = store.get_chunk(&root_hash)?;
        atomic_write(catalog_path.as_path(), buffer.as_slice())?;
    }

    let catalog = catalog_builder.open(catalog_path)?;
    catalog.show_stats();

    Ok((catalog, store))
}

struct Engine<C, S> {
    catalog: C,
    store: Rc<RefCell<S>>,
    open_dirs: HashMap<u64, Vec<(PathBuf, u64, FileType)>>,
    file_workspaces: HashMap<u64, FileWorkspace<S>>,
}

impl<C, S> Engine<C, S> {
    fn handle_request(&mut self, request: Request, chan: &ReplyChannel)
    where
        C: Catalog,
        S: Store,
    {
        match request {
            Request::GetAttr { index } => {
                let reply = self.catalog
                    .get_inode(index)
                    .map(|inode| inode.attributes)
                    .map_err(|e| e.context(EngineError::GetAttr(index)).into());
                let _ = chan.send(Reply::GetAttr(reply));
            }
            Request::Lookup { parent, name } => {
                let reply = self.catalog
                    .get_dir_entry_inode(parent, PathBuf::from(&name).as_path())
                    .map(|inode| inode.attributes)
                    .map_err(|e| e.context(EngineError::Lookup(parent, name.clone())).into());
                let _ = chan.send(Reply::Lookup(reply));
            }
            Request::OpenDir { index, flags } => {
                let rw = (O_WRONLY | O_RDWR) as u32;
                let reply = {
                    if (flags & rw) > 0 {
                        Err(EngineError::Access(index).into())
                    } else {
                        self.catalog
                            .get_dir_entries(index)
                            .map(|entries| {
                                let entries = entries
                                    .iter()
                                    .map(|&(ref name, idx)| {
                                        if let Ok(inode) = self.catalog.get_inode(idx) {
                                            (name.clone(), idx, inode.attributes.kind)
                                        } else {
                                            panic!(
                                                "Fatal engine error. Could not retrieve inode {}",
                                                idx
                                            )
                                        }
                                    })
                                    .collect::<Vec<_>>();
                                self.open_dirs.insert(index, entries);
                            })
                            .map_err(|e| e.context(EngineError::DirOpen(index)).into())
                    }
                };
                let _ = chan.send(Reply::OpenDir(reply));
            }
            Request::ReleaseDir { index, .. } => {
                let reply = self.open_dirs
                    .remove(&index)
                    .map(|_| ())
                    .ok_or_else(|| EngineError::DirClose(index).into());
                let _ = chan.send(Reply::ReleaseDir(reply));
            }
            Request::ReadDir { index, .. } => {
                let reply = self.open_dirs
                    .get(&index)
                    .cloned()
                    .ok_or_else(|| EngineError::DirRead(index).into());
                let _ = chan.send(Reply::ReadDir(reply));
            }
            Request::OpenFile { index, flags } => {
                let rw = (O_WRONLY | O_RDWR) as u32;
                let reply = {
                    if (flags & rw) > 0 {
                        Err(EngineError::Access(index).into())
                    } else {
                        self.catalog
                            .get_inode(index)
                            .map(|inode| {
                                if !self.file_workspaces.contains_key(&index) {
                                    self.file_workspaces.insert(
                                        index,
                                        FileWorkspace::new(&inode, Rc::clone(&self.store)),
                                    );
                                }
                            })
                            .map_err(|e| e.context(EngineError::FileOpen(index)).into())
                    }
                };
                let _ = chan.send(Reply::OpenFile(reply));
            }
            Request::ReadData {
                index,
                offset,
                size,
            } => {
                let offset = ::std::cmp::max(offset, 0) as usize;
                let reply = self.file_workspaces
                    .get(&index)
                    .ok_or_else(|| EngineError::FileRead(index).into())
                    .and_then(|ws| ws.read(offset, size as usize))
                    .map_err(|e| e.context(EngineError::FileRead(index)).into());
                let _ = chan.send(Reply::ReadData(reply));
            }
            Request::ReleaseFile { index, .. } => {
                let reply = self.file_workspaces
                    .get_mut(&index)
                    .ok_or_else(|| EngineError::FileClose(index).into())
                    .and_then(|ws| Ok(ws.unload()));
                let _ = chan.send(Reply::ReleaseFile(reply));
            }
        }
    }
}

// TODO: bring back test when Engine is fixed for in-memory catalogs and stores
/*
#[cfg(test)]
mod tests {
    use catalog::MemCatalogBuilder;
    use store::MemStoreBuilder;

    use super::*;

    #[test]
    fn engine_works() {
        let cb = MemCatalogBuilder;
        let sb = MemStoreBuilder;
        assert!(start_engine(&cb, &sb, &PathBuf::new(), None, 1000, 1000).is_ok());
    }
}
 */
