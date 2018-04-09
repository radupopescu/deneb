use std::{ffi::OsString, path::PathBuf, sync::mpsc::SyncSender};

use inode::{FileAttributeChanges, FileAttributes, FileType};
use errors::DenebResult;

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
    SetAttr {
        index: u64,
        changes: FileAttributeChanges,
    },
    Lookup {
        parent: u64,
        name: OsString,
    },
    OpenDir {
        index: u64,
        #[allow(dead_code)]
        flags: u32,
    },
    ReleaseDir {
        index: u64,
        #[allow(dead_code)]
        flags: u32,
    },
    ReadDir {
        index: u64,
        #[allow(dead_code)]
        offset: i64,
    },
    OpenFile {
        index: u64,
        #[allow(dead_code)]
        flags: u32,
    },
    ReadData {
        index: u64,
        offset: i64,
        size: u32,
    },
    WriteData {
        index: u64,
        offset: i64,
        data: Vec<u8>,
    },
    ReleaseFile {
        index: u64,
        #[allow(dead_code)]
        flags: u32,
        #[allow(dead_code)]
        lock_owner: u64,
        #[allow(dead_code)]
        flush: bool,
    },
    CreateFile {
        parent: u64,
        name: OsString,
        mode: u32,
        flags: u32,
    },
}

pub(in engine) enum Reply {
    GetAttr(DenebResult<FileAttributes>),
    SetAttr(DenebResult<FileAttributes>),
    Lookup(DenebResult<FileAttributes>),
    OpenDir(DenebResult<()>),
    ReleaseDir(DenebResult<()>),
    ReadDir(DenebResult<Vec<(PathBuf, u64, FileType)>>),
    OpenFile(DenebResult<()>),
    ReadData(DenebResult<Vec<u8>>),
    WriteData(DenebResult<u32>),
    ReleaseFile(DenebResult<()>),
    CreateFile(DenebResult<(u64, FileAttributes)>),
}

pub(in engine) type ReplyChannel = SyncSender<Reply>;
pub(in engine) type RequestChannel = SyncSender<(Request, ReplyChannel)>;
