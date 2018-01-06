use futures::sync::mpsc::Sender as FutureSender;

use std::path::PathBuf;
use std::sync::mpsc::Sender as StdSender;

use be::cas::Digest;
use be::inode::{ChunkDescriptor, INode};
use common::errors::DenebResult;

pub (in be::engine) enum Request {
    GetNextIndex,
    GetINode {
        index: u64,
    },
    GetDirEntryIndex {
        parent: u64,
        name: PathBuf,
    },
    GetDirEntryINode {
        parent: u64,
        name: PathBuf,
    },
    GetDirEntries {
        parent: u64,
    },
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

    GetChunk {
        digest: Digest,
    },
    PutChunk {
        digest: Digest,
        contents: Vec<u8>,
    },
}

pub (in be::engine) enum Reply {
    NextIndex(u64),
    INode(DenebResult<INode>),
    Index(DenebResult<u64>),
    DirEntries(DenebResult<Vec<(PathBuf, u64)>>),

    Chunk(DenebResult<Vec<u8>>),

    Result(DenebResult<()>),
}

pub (in be::engine) type ReplyChannel = StdSender<Reply>;
pub (in be::engine) type RequestChannel = FutureSender<(Request, ReplyChannel)>;
