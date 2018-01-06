use futures::{Future, Sink};

use std::path::{Path, PathBuf};
use std::sync::mpsc::channel as std_channel;

use be::cas::Digest;
use be::catalog::Catalog;
use be::inode::{ChunkDescriptor, INode};
use be::store::Store;
use common::errors::DenebResult;

use super::protocol::{Reply, Request, RequestChannel};

#[derive(Clone)]
pub struct Handle {
    channel: RequestChannel,
}

impl Handle {
    pub (in be::engine) fn new(channel: RequestChannel) -> Handle {
        Handle { channel }
    }

    fn make_request(&self, req: Request) -> DenebResult<Reply> {
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

    fn get_inode(&self, index: u64) -> DenebResult<INode> {
        if let Reply::INode(result) = self.make_request(Request::GetINode { index: index })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> DenebResult<u64> {
        if let Reply::Index(result) = self.make_request(Request::GetDirEntryIndex {
            parent: parent,
            name: name.to_owned(),
        })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn get_dir_entry_inode(&self, parent: u64, name: &Path) -> DenebResult<INode> {
        if let Reply::INode(result) = self.make_request(Request::GetDirEntryINode {
            parent: parent,
            name: name.to_owned(),
        })? {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn get_dir_entries(&self, parent: u64) -> DenebResult<Vec<(PathBuf, u64)>> {
        if let Reply::DirEntries(result) =
            self.make_request(Request::GetDirEntries { parent: parent })?
        {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }

    fn add_inode(
        &mut self,
        entry: &Path,
        index: u64,
        chunks: Vec<ChunkDescriptor>,
    ) -> DenebResult<()> {
        if let Ok(Reply::Result(result)) = self.make_request(Request::AddINode {
            entry: entry.to_owned(),
            index: index,
            chunks: chunks,
        }) {
            result
        } else {
            bail!("Invalid reply received from engine")
        }
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> DenebResult<()> {
        if let Ok(Reply::Result(result)) = self.make_request(Request::AddDirEntry {
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
    fn get_chunk(&self, digest: &Digest) -> DenebResult<Vec<u8>> {
        if let Reply::Chunk(result) = self.make_request(Request::GetChunk {
            digest: digest.clone(),
        })? {
            result
        } else {
            bail!("Invalid reply received from engine")
        }
    }

    fn put_chunk(&mut self, digest: Digest, contents: &[u8]) -> DenebResult<()> {
        if let Ok(Reply::Result(result)) = self.make_request(Request::PutChunk {
            digest: digest.clone(),
            contents: contents.to_owned(),
        }) {
            result
        } else {
            bail!("Invalid reply received from engine.")
        }
    }
}
