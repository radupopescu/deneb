use futures::sync::mpsc::Sender as FutureSender;

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::mpsc::Sender as StdSender;

use inode::{FileAttributes, FileType};
use deneb_common::errors::DenebResult;

pub struct RequestId {
    pub unique_id: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

pub(in engine) enum Request {
    GetAttr {
        index: u64,
    },
    Lookup {
        parent: u64,
        name: OsString,
    },
    OpenDir {
        index: u64,
        #[allow(dead_code)] flags: u32,
    },
    ReleaseDir {
        index: u64,
        #[allow(dead_code)] flags: u32,
    },
    ReadDir {
        index: u64,
        #[allow(dead_code)] offset: i64,
    },
    OpenFile {
        index: u64,
        flags: u32,
    },
    ReadData {
        index: u64,
        offset: i64,
        size: u32,
    },
    ReleaseFile {
        index: u64,
        #[allow(dead_code)] flags: u32,
        #[allow(dead_code)] lock_owner: u64,
        #[allow(dead_code)] flush: bool,
    },
}

pub(in engine) enum Reply {
    GetAttr(DenebResult<FileAttributes>),
    Lookup(DenebResult<FileAttributes>),
    OpenDir(DenebResult<()>),
    ReleaseDir(DenebResult<()>),
    ReadDir(DenebResult<Vec<(PathBuf, u64, FileType)>>),
    OpenFile(DenebResult<()>),
    ReadData(DenebResult<Vec<u8>>),
    ReleaseFile(DenebResult<()>),
}

pub(in engine) type ReplyChannel = StdSender<Reply>;
pub(in engine) type RequestChannel = FutureSender<(Request, ReplyChannel)>;
