use std::{ffi::OsStr, path::PathBuf};

use errors::DenebResult;
use inode::{FileAttributeChanges, FileAttributes, FileType};

use super::{
    protocol::{call, cast, RequestChannel},
    requests::{
        CreateDir, CreateFile, GetAttr, Lookup, OpenDir, OpenFile, ReadData, ReadDir, ReleaseDir,
        ReleaseFile, RemoveDir, Rename, RequestId, SetAttr, Unlink, WriteData, Ping,
    },
    Engine,
};

#[derive(Clone)]
pub struct Handle {
    channel: RequestChannel<Engine>,
}

impl Handle {
    // Client API
    pub fn get_attr(&self, _id: &RequestId, index: u64) -> DenebResult<FileAttributes> {
        call(GetAttr { index }, &self.channel)
    }

    pub fn set_attr(
        &self,
        _id: &RequestId,
        index: u64,
        changes: FileAttributeChanges,
    ) -> DenebResult<FileAttributes> {
        call(SetAttr { index, changes }, &self.channel)
    }

    pub fn lookup(
        &self,
        _id: &RequestId,
        parent: u64,
        name: &OsStr,
    ) -> DenebResult<Option<FileAttributes>> {
        call(
            Lookup {
                parent,
                name: name.to_os_string(),
            },
            &self.channel,
        )
    }

    pub fn open_dir(&self, _id: &RequestId, index: u64, flags: u32) -> DenebResult<()> {
        call(OpenDir { index, flags }, &self.channel)
    }

    pub fn release_dir(&self, _id: &RequestId, index: u64, flags: u32) -> DenebResult<()> {
        call(ReleaseDir { index, flags }, &self.channel)
    }

    pub fn read_dir(
        &self,
        _id: &RequestId,
        index: u64,
        offset: i64,
    ) -> DenebResult<Vec<(PathBuf, u64, FileType)>> {
        call(ReadDir { index, offset }, &self.channel)
    }

    pub fn open_file(&self, _id: &RequestId, index: u64, flags: u32) -> DenebResult<()> {
        call(OpenFile { index, flags }, &self.channel)
    }

    pub fn read_data(
        &self,
        _id: &RequestId,
        index: u64,
        offset: i64,
        size: u32,
    ) -> DenebResult<Vec<u8>> {
        call(
            ReadData {
                index,
                offset,
                size,
            },
            &self.channel,
        )
    }

    pub fn write_data(
        &self,
        _id: &RequestId,
        index: u64,
        offset: i64,
        data: &[u8],
    ) -> DenebResult<u32> {
        call(
            WriteData {
                index,
                offset,
                data: data.to_vec(),
            },
            &self.channel,
        )
    }

    pub fn release_file(
        &self,
        _id: &RequestId,
        index: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> DenebResult<()> {
        call(
            ReleaseFile {
                index,
                flags,
                lock_owner,
                flush,
            },
            &self.channel,
        )
    }

    pub fn create_file(
        &self,
        _id: &RequestId,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> DenebResult<(u64, FileAttributes)> {
        call(
            CreateFile {
                parent,
                name: name.to_owned(),
                mode,
                flags,
            },
            &self.channel,
        )
    }

    pub fn create_dir(
        &self,
        _id: &RequestId,
        parent: u64,
        name: &OsStr,
        mode: u32,
    ) -> DenebResult<FileAttributes> {
        call(
            CreateDir {
                parent,
                name: name.to_owned(),
                mode,
            },
            &self.channel,
        )
    }

    pub fn unlink(&self, _id: &RequestId, parent: u64, name: &OsStr) -> DenebResult<()> {
        call(
            Unlink {
                parent,
                name: name.to_owned(),
            },
            &self.channel,
        )
    }

    pub fn remove_dir(&self, _id: &RequestId, parent: u64, name: &OsStr) -> DenebResult<()> {
        call(
            RemoveDir {
                parent,
                name: name.to_owned(),
            },
            &self.channel,
        )
    }

    pub fn rename(
        &self,
        _id: &RequestId,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> DenebResult<()> {
        call(
            Rename {
                parent,
                name: name.to_owned(),
                new_parent,
                new_name: new_name.to_owned(),
            },
            &self.channel,
        )
    }

    pub fn ping(&self) {
        cast(Ping, &self.channel);
    }

    // Private functions
    pub(in engine) fn new(channel: RequestChannel<Engine>) -> Handle {
        Handle { channel }
    }
}
