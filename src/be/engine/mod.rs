use futures::Stream;
use futures::sync::mpsc::channel as future_channel;
use time::now_utc;
use tokio_core::reactor::Core;

use std::fs::{create_dir_all, File};
use std::io::Read;

use be::cas::hash;
use be::catalog::{Catalog, CatalogBuilder};
use be::manifest::Manifest;
use be::populate_with_dir;
use be::store::{Store, StoreBuilder};
use common::errors::DenebResult;
use common::atomic_write;

use std::path::{Path, PathBuf};
use std::thread::spawn as tspawn;

mod protocol;
mod handle;

use self::protocol::{Reply, ReplyChannel, Request, RequestChannel};
use self::handle::Handle;

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
                let handler = rx.for_each(move |(event, tx)| {
                    handle_request(event, &tx, &mut catalog, &mut store);
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

fn handle_request<C, S>(request: Request, chan: &ReplyChannel, catalog: &mut C, store: &mut S)
where
    C: Catalog,
    S: Store,
{
    match request {
        // Catalog operations
        Request::GetNextIndex => {
            let _ = chan.send(Reply::NextIndex(catalog.get_next_index()));
        }
        Request::GetINode { index } => {
            let _ = chan.send(Reply::INode(catalog.get_inode(index)));
        }
        Request::GetDirEntryIndex { parent, name } => {
            let _ = chan.send(Reply::Index(
                catalog.get_dir_entry_index(parent, name.as_path()),
            ));
        }
        Request::GetDirEntryINode { parent, name } => {
            let _ = chan.send(Reply::INode(
                catalog.get_dir_entry_inode(parent, name.as_path()),
            ));
        }
        Request::GetDirEntries { parent } => {
            let _ = chan.send(Reply::DirEntries(catalog.get_dir_entries(parent)));
        }

        Request::AddINode {
            entry,
            index,
            chunks,
        } => {
            let _ = chan.send(Reply::Result(catalog.add_inode(
                entry.as_path(),
                index,
                chunks,
            )));
        }

        Request::AddDirEntry {
            parent,
            name,
            index,
        } => {
            let _ = chan.send(Reply::Result(catalog.add_dir_entry(
                parent,
                name.as_path(),
                index,
            )));
        }

        // Store operations
        Request::GetChunk { digest } => {
            let _ = chan.send(Reply::Chunk(store.get_chunk(&digest)));
        }
        Request::PutChunk { digest, contents } => {
            let _ = chan.send(Reply::Result(store.put_chunk(digest, contents.as_slice())));
        }
    }
}

#[cfg(test)]
mod tests {
    use be::cas::hash;
    use be::catalog::MemCatalogBuilder;
    use be::store::MemStoreBuilder;

    use super::*;

    #[test]
    fn engine_works() {
        let cb = MemCatalogBuilder;
        let sb = MemStoreBuilder;
        if let Ok(engine) = Engine::new(&cb, &sb, &PathBuf::new(), None, 1000, 1000) {
            let h = engine.handle();

            assert!(h.get_inode(0).is_err());
            let digest = hash(&[]);
            assert!(h.get_chunk(&digest).is_err());
        }
    }
}
