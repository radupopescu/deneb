use futures::Stream;
use futures::sync::mpsc::channel as future_channel;
use nix::libc::{O_RDWR, O_WRONLY};
use time::now_utc;
use tokio_core::reactor::Core;

use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::Read;

use be::cas::hash;
use be::catalog::{Catalog, CatalogBuilder};
use be::inode::{lookup_chunks, ChunkPart, FileType};
use be::manifest::Manifest;
use be::populate_with_dir;
use be::store::{Store, StoreBuilder};
use common::errors::{DenebResult, EngineError};
use common::atomic_write;

use std::path::{Path, PathBuf};
use std::thread::spawn as tspawn;

mod protocol;
mod handle;

use self::protocol::{Reply, ReplyChannel, Request, RequestChannel};

pub use self::protocol::RequestId;
pub use self::handle::Handle;

struct OpenFileContext;

pub struct Engine {
    requests: RequestChannel,
}

impl Engine {
    pub fn new<CB, SB>(
        catalog_builder: &CB,
        store_builder: &SB,
        work_dir: &Path,
        sync_dir: Option<PathBuf>,
        chunk_size: usize,
        queue_size: usize,
    ) -> DenebResult<Engine>
    where
        CB: CatalogBuilder,
        <CB as CatalogBuilder>::Catalog: Send + 'static,
        SB: StoreBuilder,
        <SB as StoreBuilder>::Store: Send + 'static,
    {
        let (mut catalog, mut store) = init(
            catalog_builder,
            store_builder,
            work_dir,
            sync_dir,
            chunk_size,
        )?;

        let (tx, rx) = future_channel(queue_size);
        let _ = tspawn(|| {
            if let Ok(mut core) = Core::new() {
                let mut open_dirs = HashMap::new();
                let mut open_files = HashMap::new();
                let handler = rx.for_each(move |(event, tx)| {
                    handle_request(
                        event,
                        &tx,
                        &mut catalog,
                        &mut store,
                        &mut open_dirs,
                        &mut open_files,
                    );
                    Ok(())
                });

                let _ = core.run(handler);
            }
        });

        Ok(Engine { requests: tx })
    }

    pub fn handle(&self) -> Handle {
        Handle::new(self.requests.clone())
    }
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
    let mut store = store_builder.at_dir(work_dir)?;

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
        let mut buffer = Vec::new();
        let _ = f.read_to_end(&mut buffer);
        let digest = hash(buffer.as_slice());
        store.put_chunk(digest.clone(), buffer.as_slice())?;

        // Create and save the repository manifest
        let manifest = Manifest::new(digest, None, now_utc());
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

fn handle_request<C, S>(
    request: Request,
    chan: &ReplyChannel,
    catalog: &mut C,
    store: &mut S,
    open_dirs: &mut HashMap<u64, Vec<(PathBuf, u64, FileType)>>,
    open_files: &mut HashMap<u64, OpenFileContext>,
) where
    C: Catalog,
    S: Store,
{
    match request {
        Request::GetAttr { index } => {
            let reply = catalog.get_inode(index).map(|inode| inode.attributes);
            let _ = chan.send(Reply::GetAttr(reply));
        }
        Request::Lookup { parent, name } => {
            let reply = catalog
                .get_dir_entry_inode(parent, PathBuf::from(name).as_path())
                .map(|inode| inode.attributes);
            let _ = chan.send(Reply::Lookup(reply));
        }
        Request::OpenDir { index, .. } => {
            let reply = catalog.get_dir_entries(index).map(|entries| {
                let entries = entries
                    .iter()
                    .map(|&(ref name, idx)| {
                        if let Ok(inode) = catalog.get_inode(idx) {
                            (name.clone(), idx, inode.attributes.kind)
                        } else {
                            panic!("Fatal engine error. Could not retrieve inode {}", idx)
                        }
                    })
                    .collect::<Vec<_>>();
                open_dirs.insert(index, entries);
            });
            let _ = chan.send(Reply::OpenDir(reply));
        }
        Request::ReleaseDir { index, .. } => {
            let reply = open_dirs
                .remove(&index)
                .map(|_| ())
                .ok_or_else(|| EngineError::DirClose(index).into());
            let _ = chan.send(Reply::ReleaseDir(reply));
        }
        Request::ReadDir { index, .. } => {
            let reply = open_dirs
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
                    catalog.get_inode(index).map(|_inode| {
                        open_files.insert(index, OpenFileContext);
                    })
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
            let reply = open_files
                .get(&index)
                .ok_or_else(|| EngineError::FileRead(index).into())
                .and_then(|_ctx| catalog.get_inode(index))
                .and_then(|inode| {
                    chunks_to_buffer(
                        &lookup_chunks(offset, size as usize, inode.chunks.as_slice()),
                        store,
                    )
                });
            let _ = chan.send(Reply::ReadData(reply));
        }
        Request::ReleaseFile { index, .. } => {
            let reply = open_files
                .remove(&index)
                .map(|_| ())
                .ok_or_else(|| EngineError::FileClose(index).into());
            let _ = chan.send(Reply::ReleaseFile(reply));
        }
    }
}

/// Fill a buffer using the list of `ChunkPart`
fn chunks_to_buffer<S: Store>(chunks: &[ChunkPart], store: &S) -> DenebResult<Vec<u8>> {
    let mut buffer = Vec::new();
    for &ChunkPart(digest, begin, end) in chunks {
        let chunk = store.get_chunk(digest)?;
        buffer.extend_from_slice(&chunk[begin..end]);
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use be::catalog::MemCatalogBuilder;
    use be::store::MemStoreBuilder;

    use super::*;

    #[test]
    fn engine_works() {
        let cb = MemCatalogBuilder;
        let sb = MemStoreBuilder;
        if let Ok(engine) = Engine::new(&cb, &sb, &PathBuf::new(), None, 1000, 1000) {
            let _ = engine.handle();
        }
    }
}
