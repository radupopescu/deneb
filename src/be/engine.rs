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

// TODO:
// + Rename blob -> chunk
// + Consolidate return types for Handle methods (all should return a Result)

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

    GetBlob { digest: Digest },
    PutBlob { digest: Digest, contents: Vec<u8> },
}

enum Reply {
    NextIndex(u64),
    INode(Option<INode>),
    Index(Option<u64>),
    DirEntries(Option<Vec<(PathBuf, u64)>>),

    Blob(Result<Option<Vec<u8>>>),

    Result(Result<()>),
}

type ReplyChannel = StdSender<Reply>;
type RequestChannel = FutureSender<(Request, ReplyChannel)>;

#[derive(Clone)]
pub struct Handle {
    channel: RequestChannel,
}

/*
impl<C, S> Catalog for Engine<C, S>
    where C: Catalog
{
}
*/
impl Handle {
    // Catalog operations

    pub fn get_next_index(&self) -> u64 {
        if let Ok(Reply::NextIndex(result)) = self.make_request(Request::GetNextIndex) {
            result
        } else {
            panic!("Invalid reply received from engine.")
        }
    }

    pub fn get_inode(&self, index: u64) -> Option<INode> {
        if let Ok(Reply::INode(result)) = self.make_request(Request::GetINode { index: index }) {
            result
        } else {
            None
        }
    }

    pub fn get_dir_entry_index(&self, parent: u64, name: &Path) -> Option<u64> {
        if let Ok(Reply::Index(result)) =
            self.make_request(Request::GetDirEntryIndex {
                                  parent: parent,
                                  name: name.to_owned(),
                              }) {
            result
        } else {
            None
        }
    }

    pub fn get_dir_entry_inode(&self, parent: u64, name: &Path) -> Option<INode> {
        if let Ok(Reply::INode(result)) =
            self.make_request(Request::GetDirEntryINode {
                                  parent: parent,
                                  name: name.to_owned(),
                              }) {
            result
        } else {
            None
        }
    }

    pub fn get_dir_entries(&self, parent: u64) -> Option<Vec<(PathBuf, u64)>> {
        if let Ok(Reply::DirEntries(result)) =
            self.make_request(Request::GetDirEntries { parent: parent }) {
            result
        } else {
            None
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

    pub fn get_blob(&self, digest: &Digest) -> Result<Option<Vec<u8>>> {
        if let Ok(Reply::Blob(result)) =
            self.make_request(Request::GetBlob { digest: digest.clone() }) {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    pub fn put_blob(&self, digest: Digest, contents: &[u8]) -> Result<()> {
        if let Ok(Reply::Result(result)) =
            self.make_request(Request::PutBlob {
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
        Request::GetBlob { digest } => {
            let _ = chan.send(Reply::Blob(store.get(&digest)));
        }
        Request::PutBlob { digest, contents } => {
            let _ = chan.send(Reply::Result(store.put(digest, contents.as_slice())));
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

        assert!(h.get_inode(0).is_none());
        let digest = hash(&[]);
        assert!(h.get_blob(&digest).is_ok());
    }
}
