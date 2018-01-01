use futures::{Future, Sink, Stream};
use futures::sync::mpsc::{Sender as FutureSender, channel as future_channel};
use time::now_utc;
use tokio_core::reactor::Core;

use std::fs::{File, create_dir_all};
use std::io::Read;
use std::sync::mpsc::{Sender as StdSender, channel as std_channel};

use be::cas::{Digest, hash};
use be::catalog::{Catalog, CatalogBuilder};
use be::inode::{INode, ChunkDescriptor};
use be::manifest::Manifest;
use be::populate_with_dir;
use be::store::{Store, StoreBuilder};
use common::errors::*;
use common::util::file::atomic_write;

use std::path::{Path, PathBuf};
use std::thread::spawn as tspawn;

enum Request {
    GetNextIndex,
    GetINode { index: u64 },
    GetDirEntryIndex { parent: u64, name: PathBuf },
    GetDirEntryINode { parent: u64, name: PathBuf },
    GetDirEntries { parent: u64 },
    AddINode {
        entry: PathBuf,
        index: u64,
        chunks: Vec<ChunkDescriptor>,
    },
    AddDirEntry {
        parent: u64,
        name: PathBuf,
        index: u64,
    },

    GetChunk { digest: Digest },
    PutChunk { digest: Digest, contents: Vec<u8> },
}

enum Reply {
    NextIndex(u64),
    INode(Result<INode>),
    Index(Result<u64>),
    DirEntries(Result<Vec<(PathBuf, u64)>>),

    Chunk(Result<Vec<u8>>),

    Result(Result<()>),
}

type ReplyChannel = StdSender<Reply>;
type RequestChannel = FutureSender<(Request, ReplyChannel)>;

#[derive(Clone)]
pub struct Handle {
    channel: RequestChannel,
}

impl Handle {
    fn make_request(&self, req: Request) -> Result<Reply> {
        let (tx, rx) = std_channel();
        if self.channel.clone().send((req, tx)).wait().is_ok() {
            rx.recv().map_err(|e| e.into())
        } else {
            bail!("Could not make request to engine.")
        }
    }
}

impl Catalog for Handle {
    fn get_next_index(&self) -> u64 {
        if let Ok(Reply::NextIndex(result)) = self.make_request(Request::GetNextIndex) {
            result
        } else {
            panic!("Did not receive new inode index from engine")
        }
    }

    fn get_inode(&self, index: u64) -> Result<INode> {
        if let Reply::INode(result) = self.make_request(Request::GetINode { index: index })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> Result<u64> {
        if let Reply::Index(result) =
            self.make_request(Request::GetDirEntryIndex {
                                  parent: parent,
                                  name: name.to_owned(),
                              })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn get_dir_entry_inode(&self, parent: u64, name: &Path) -> Result<INode> {
        if let Reply::INode(result) =
            self.make_request(Request::GetDirEntryINode {
                                  parent: parent,
                                  name: name.to_owned(),
                              })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn get_dir_entries(&self, parent: u64) -> Result<Vec<(PathBuf, u64)>> {
        if let Reply::DirEntries(result) =
            self.make_request(Request::GetDirEntries { parent: parent })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn add_inode(&mut self, entry: &Path, index: u64, chunks: Vec<ChunkDescriptor>) -> Result<()> {
        if let Ok(Reply::Result(result)) =
            self.make_request(Request::AddINode {
                                  entry: entry.to_owned(),
                                  index: index,
                                  chunks: chunks,
                              }) {
            result
        } else {
            bail!("Invalid reply received from engine")
        }
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> Result<()> {
        if let Ok(Reply::Result(result)) =
            self.make_request(Request::AddDirEntry {
                                  parent: parent,
                                  name: name.to_owned(),
                                  index: index,
                              }) {
            result
        } else {
            bail!("Invalid reply received from engine")
        }
    }
}

impl Store for Handle {
    fn get_chunk(&self, digest: &Digest) -> Result<Vec<u8>> {
        if let Reply::Chunk(result) =
            self.make_request(Request::GetChunk { digest: digest.clone() })? {
            result
        } else {
            bail!("Invalid reply received from engine")
        }
    }

    fn put_chunk(&mut self, digest: Digest, contents: &[u8]) -> Result<()> {
        if let Ok(Reply::Result(result)) =
            self.make_request(Request::PutChunk {
                                  digest: digest.clone(),
                                  contents: contents.to_owned(),
                              }) {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }
}

pub struct Engine {
    requests: RequestChannel,
}

impl Engine {
    pub fn new<CB, SB>(catalog_builder: CB,
                       store_builder: SB,
                       work_dir: PathBuf,
                       sync_dir: Option<PathBuf>,
                       chunk_size: usize,
                       queue_size: usize)
                       -> Result<Engine>
        where CB: CatalogBuilder,
              <CB as CatalogBuilder>::Catalog: Send + 'static,
              SB: StoreBuilder,
              <SB as StoreBuilder>::Store: Send + 'static
    {
        let (mut catalog, mut store) = init(&catalog_builder,
                                            &store_builder,
                                            &work_dir,
                                            sync_dir,
                                            chunk_size)?;

        let (tx, rx) = future_channel(queue_size);
        let _ = tspawn(|| if let Ok(mut core) = Core::new() {
                           let handler =
                               rx.for_each(move |(event, tx)| {
                                               handle_request(event, &tx, &mut catalog, &mut store);
                                               Ok(())
                                           });

                           let _ = core.run(handler);
                       });

        Ok(Engine { requests: tx })
    }

    pub fn handle(&self) -> Handle {
        Handle { channel: self.requests.clone() }
    }
}

fn init<CB, SB>(catalog_builder: &CB,
                store_builder: &SB,
                work_dir: &PathBuf,
                sync_dir: Option<PathBuf>,
                chunk_size: usize)
                -> Result<(CB::Catalog, SB::Store)>
    where CB: CatalogBuilder,
          SB: StoreBuilder
{
    // Create an object store
    let mut store = store_builder.at_dir(work_dir.as_path())?;

    let catalog_root = work_dir.as_path().to_owned().join("scratch");
    create_dir_all(catalog_root.as_path())?;
    let catalog_path = catalog_root.join("current_catalog");
    info!("Catalog path: {:?}", catalog_path);

    let manifest_path = work_dir.as_path().to_owned().join("manifest");
    info!("Manifest path: {:?}", manifest_path);

    // Create the file metadata catalog and populate it with the contents of "sync_dir"
    if let Some(sync_dir) = sync_dir {
        {
            let mut catalog = catalog_builder.create(catalog_path.as_path())?;
            populate_with_dir(&mut catalog, &mut store, sync_dir.as_path(), chunk_size)?;
            info!("Catalog populated with contents of {:?}",
                  sync_dir.as_path());
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
    where C: Catalog,
          S: Store
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
            let _ = chan.send(Reply::Index(catalog.get_dir_entry_index(parent, name.as_path())));
        }
        Request::GetDirEntryINode { parent, name } => {
            let _ = chan.send(Reply::INode(catalog.get_dir_entry_inode(parent, name.as_path())));
        }
        Request::GetDirEntries { parent } => {
            let _ = chan.send(Reply::DirEntries(catalog.get_dir_entries(parent)));
        }

        Request::AddINode {
            entry,
            index,
            chunks,
        } => {
            let _ = chan.send(Reply::Result(catalog.add_inode(entry.as_path(), index, chunks)));
        }

        Request::AddDirEntry {
            parent,
            name,
            index,
        } => {
            let _ = chan.send(Reply::Result(catalog.add_dir_entry(parent, name.as_path(), index)));
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
        if let Ok(engine) = Engine::new(cb, sb, PathBuf::new(), None, 1000, 1000) {
            let h = engine.handle();

            assert!(h.get_inode(0).is_err());
            let digest = hash(&[]);
            assert!(h.get_chunk(&digest).is_err());
        }
    }
}
