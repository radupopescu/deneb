use time::Timespec;

use std::{ffi::OsStr, path::PathBuf, sync::mpsc::sync_channel};

use inode::{FileAttributes, FileType};
use errors::{DenebResult, EngineError};

use super::protocol::{Reply, Request, RequestChannel, RequestId};

#[derive(Clone)]
pub struct Handle {
    channel: RequestChannel,
}

impl Handle {
    // Client API
    pub fn get_attr(&self, _id: &RequestId, index: u64) -> DenebResult<FileAttributes> {
        let reply = self.make_request(Request::GetAttr { index })?;
        if let Reply::GetAttr(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn set_attr(
        &self,
        _id: &RequestId,
        index: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        flags: Option<u32>,
    ) -> DenebResult<FileAttributes> {
        let reply = self.make_request(Request::SetAttr {
            index,
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            crtime,
            chgtime,
            flags,
        })?;
        if let Reply::SetAttr(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn lookup(
        &self,
        _id: &RequestId,
        parent: u64,
        name: &OsStr,
    ) -> DenebResult<FileAttributes> {
        let reply = self.make_request(Request::Lookup {
            parent: parent,
            name: name.to_os_string(),
        })?;
        if let Reply::Lookup(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn open_dir(&self, _id: &RequestId, index: u64, flags: u32) -> DenebResult<()> {
        let reply = self.make_request(Request::OpenDir { index, flags })?;
        if let Reply::OpenDir(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn release_dir(&self, _id: &RequestId, index: u64, flags: u32) -> DenebResult<()> {
        let reply = self.make_request(Request::ReleaseDir { index, flags })?;
        if let Reply::ReleaseDir(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn read_dir(
        &self,
        _id: &RequestId,
        index: u64,
        offset: i64,
    ) -> DenebResult<Vec<(PathBuf, u64, FileType)>> {
        let reply = self.make_request(Request::ReadDir { index, offset })?;
        if let Reply::ReadDir(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn open_file(&self, _id: &RequestId, index: u64, flags: u32) -> DenebResult<()> {
        let reply = self.make_request(Request::OpenFile { index, flags })?;
        if let Reply::OpenFile(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn read_data(
        &self,
        _id: &RequestId,
        index: u64,
        offset: i64,
        size: u32,
    ) -> DenebResult<Vec<u8>> {
        let reply = self.make_request(Request::ReadData {
            index,
            offset,
            size,
        })?;
        if let Reply::ReadData(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn write_data(
        &self,
        _id: &RequestId,
        index: u64,
        offset: i64,
        data: &[u8],
    ) -> DenebResult<u32> {
        let reply = self.make_request(Request::WriteData {
            index,
            offset,
            data: data.to_vec(),
        })?;
        if let Reply::WriteData(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    pub fn release_file(
        &self,
        _id: &RequestId,
        index: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> DenebResult<()> {
        let reply = self.make_request(Request::ReleaseFile {
            index,
            flags,
            lock_owner,
            flush,
        })?;
        if let Reply::ReleaseFile(result) = reply {
            result
        } else {
            Err(EngineError::InvalidReply.into())
        }
    }

    // Private functions
    pub(in engine) fn new(channel: RequestChannel) -> Handle {
        Handle { channel }
    }

    fn make_request(&self, req: Request) -> DenebResult<Reply> {
        let (tx, rx) = sync_channel(0);
        self.channel
            .clone()
            .send((req, tx))
            .map_err(|_| EngineError::SendFailed)?;
        rx.recv().map_err(|e| e.into())
    }
}
