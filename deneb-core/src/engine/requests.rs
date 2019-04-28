use {
    super::protocol::Request,
    crate::{
        inode::{FileAttributeChanges, FileAttributes, FileType},
        workspace::CommitSummary,
    },
    std::{ffi::OsString, path::PathBuf},
};

pub struct RequestId {
    pub unique_id: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

pub(in crate::engine) struct GetAttr {
    pub index: u64,
}

impl Request for GetAttr {
    type Reply = FileAttributes;
}

pub(in crate::engine) struct SetAttr {
    pub index: u64,
    pub changes: FileAttributeChanges,
}

impl Request for SetAttr {
    type Reply = FileAttributes;
}

pub(in crate::engine) struct Lookup {
    pub parent: u64,
    pub name: OsString,
}

impl Request for Lookup {
    type Reply = Option<FileAttributes>;
}

pub(in crate::engine) struct OpenDir {
    pub index: u64,
    #[allow(dead_code)]
    pub flags: u32,
}

impl Request for OpenDir {
    type Reply = ();
}

pub(in crate::engine) struct ReleaseDir {
    pub index: u64,
    #[allow(dead_code)]
    pub flags: u32,
}

impl Request for ReleaseDir {
    type Reply = ();
}

pub(in crate::engine) struct ReadDir {
    pub index: u64,
    #[allow(dead_code)]
    pub offset: i64,
}

impl Request for ReadDir {
    type Reply = Vec<(PathBuf, u64, FileType)>;
}

pub(in crate::engine) struct OpenFile {
    pub index: u64,
    #[allow(dead_code)]
    pub flags: u32,
}

impl Request for OpenFile {
    type Reply = ();
}

pub(in crate::engine) struct ReadData {
    pub index: u64,
    pub offset: i64,
    pub size: u32,
}

impl Request for ReadData {
    type Reply = Vec<u8>;
}

pub(in crate::engine) struct WriteData {
    pub index: u64,
    pub offset: i64,
    pub data: Vec<u8>,
}

impl Request for WriteData {
    type Reply = u32;
}

pub(in crate::engine) struct ReleaseFile {
    pub index: u64,
    #[allow(dead_code)]
    pub flags: u32,
    #[allow(dead_code)]
    pub lock_owner: u64,
    #[allow(dead_code)]
    pub flush: bool,
}

impl Request for ReleaseFile {
    type Reply = ();
}

pub(in crate::engine) struct CreateFile {
    pub parent: u64,
    pub name: OsString,
    pub mode: u32,
    pub flags: u32,
}

impl Request for CreateFile {
    type Reply = (u64, FileAttributes);
}

pub(in crate::engine) struct CreateDir {
    pub parent: u64,
    pub name: OsString,
    pub mode: u32,
}

impl Request for CreateDir {
    type Reply = FileAttributes;
}

pub(in crate::engine) struct Unlink {
    pub parent: u64,
    pub name: OsString,
}

impl Request for Unlink {
    type Reply = ();
}

pub(in crate::engine) struct RemoveDir {
    pub parent: u64,
    pub name: OsString,
}

impl Request for RemoveDir {
    type Reply = ();
}

pub(in crate::engine) struct Rename {
    pub parent: u64,
    pub name: OsString,
    pub new_parent: u64,
    pub new_name: OsString,
}

impl Request for Rename {
    type Reply = ();
}

pub(in crate::engine) struct Commit;

impl Request for Commit {
    type Reply = CommitSummary;
}

pub(in crate::engine) struct Ping;

impl Request for Ping {
    type Reply = String;
}

pub(in crate::engine) struct StopEngine;

impl Request for StopEngine {
    type Reply = ();
}
