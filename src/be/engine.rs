use futures::{Future, Sink, Stream};
use futures::sync::mpsc::{Sender as FutureSender, channel as future_channel};
use tokio_core::reactor::Core;

use std::sync::mpsc::{Sender as StdSender, channel as std_channel};

use be::cas::Digest;
use be::catalog::Catalog;
use be::inode::{INode, Chunk};
use be::store::Store;
use common::errors::*;

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
        chunks: Vec<Chunk>,
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
    NextIndex(Result<u64>),
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
    // Catalog operations

    pub fn get_next_index(&self) -> Result<u64> {
        if let Reply::NextIndex(result) = self.make_request(Request::GetNextIndex)? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    pub fn get_inode(&self, index: u64) -> Result<INode> {
        if let Reply::INode(result) = self.make_request(Request::GetINode { index: index })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    pub fn get_dir_entry_index(&self, parent: u64, name: &Path) -> Result<u64> {
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

    pub fn get_dir_entry_inode(&self, parent: u64, name: &Path) -> Result<INode> {
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

    pub fn get_dir_entries(&self, parent: u64) -> Result<Vec<(PathBuf, u64)>> {
        if let Reply::DirEntries(result) =
            self.make_request(Request::GetDirEntries { parent: parent })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    pub fn add_inode(&mut self, entry: &Path, index: u64, chunks: Vec<Chunk>) -> Result<()> {
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

    pub fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> Result<()> {
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

    // Store operations

    pub fn get_chunk(&self, digest: &Digest) -> Result<Vec<u8>> {
        if let Reply::Chunk(result) =
            self.make_request(Request::GetChunk { digest: digest.clone() })? {
            result
        } else {
            bail!("Invalid reply received from engine")
        }
    }

    pub fn put_chunk(&self, digest: Digest, contents: &[u8]) -> Result<()> {
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

    fn make_request(&self, req: Request) -> Result<Reply> {
        let (tx, rx) = std_channel();
        if let Ok(_) = self.channel.clone().send((req, tx)).wait() {
            rx.recv().map_err(|e| e.into())
        } else {
            bail!("Could not make request to engine.")
        }
    }
}

pub struct Engine {
    requests: RequestChannel,
}

impl Engine {
    pub fn new<C, S>(mut catalog: C, mut store: S, queue_size: usize) -> Engine
        where C: Catalog + Send + 'static,
              S: Store + Send + 'static
    {
        let (tx, rx) = future_channel(queue_size);

        let _ = tspawn(|| if let Ok(mut core) = Core::new() {
                           let handler =
                               rx.for_each(move |(event, tx)| {
                                               handle_request(event, tx, &mut catalog, &mut store);
                                               Ok(())
                                           });
                           let _ = core.run(handler);
                       });

        Engine { requests: tx }
    }

    pub fn handle(&self) -> Handle {
        Handle { channel: self.requests.clone() }
    }
}

fn handle_request<C, S>(request: Request, chan: ReplyChannel, catalog: &mut C, store: &mut S)
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
    use be::catalog::MemCatalog;
    use be::store::MemStore;

    use super::*;

    #[test]
    fn engine_works() {
        let catalog = MemCatalog::new();
        let store = MemStore::new();
        let engine = Engine::new(catalog, store, 1000);
        let h = engine.handle();

        assert!(h.get_inode(0).is_err());
        let digest = hash(&[]);
        assert!(h.get_chunk(&digest).is_err());
    }
}
