use failure::Error;
use nix;

use std::ffi::OsString;
use std::path::PathBuf;

pub type DenebResult<T> = ::std::result::Result<T, ::failure::Error>;

// Errors from the nix crate

#[derive(Debug, Fail)]
#[fail(display = "Nix error: {}", inner)]
pub struct UnixError {
    #[cause]
    inner: nix::Error,
}

impl From<nix::Error> for UnixError {
    fn from(ne: nix::Error) -> UnixError {
        UnixError { inner: ne }
    }
}

// Common Deneb errors

#[derive(Debug, Fail)]
pub enum DenebError {
    #[fail(display = "Disk IO error")]
    DiskIO,
    #[fail(display = "Command line parameter parsing error: {}", _0)]
    CommandLineParameter(String),
    #[fail(display = "Directory visit error: {:?}", _0)]
    DirectoryVisit(PathBuf),
    #[fail(display = "Invalid path encountered: {:?}", _0)]
    InvalidPath(PathBuf),
    #[fail(display = "Index generator error")]
    IndexGenerator,
    #[fail(display = "Digest read error")]
    DigestFromSlice,
}

// Object store errors

#[derive(Debug, Fail)]
pub enum StoreError {
    #[fail(display = "Get error for: {}", _0)]
    ChunkGet(String),
    #[fail(display = "Put error for: {}", _0)]
    ChunkPut(String),
}

// Catalog errors

#[derive(Debug, Fail)]
pub enum CatalogError {
    #[fail(display = "INode serialization error for index: {}", _0)]
    INodeSerialization(u64),
    #[fail(display = "Dir entry serialization error for index: {}", _0)]
    DEntrySerialization(u64),
    #[fail(display = "INode deserialization error for index: {}", _0)]
    INodeDeserialization(u64),
    #[fail(display = "Dir entry deserialization error for index: {}", _0)]
    DEntryDeserialization(u64),
    #[fail(display = "INode read error for index: {}", _0)]
    INodeRead(u64),
    #[fail(display = "INode write error for index: {}", _0)]
    INodeWrite(u64),
    #[fail(display = "Dir entry read error for index: {}", _0)]
    DEntryRead(u64),
    #[fail(display = "Dir entry write error for index: {}", _0)]
    DEntryWrite(u64),
    #[fail(display = "Dir entry {:?} not found at index: {}", _0, _1)]
    DEntryNotFound(OsString, u64),
    #[fail(display = "Invalid catalog version: {}", _0)]
    Version(u32),
}

// Engine errors

#[derive(Debug, Fail)]
pub enum EngineError {
    #[fail(display = "Failed to retrieve inode for: {}", _0)]
    GetINode(u64),
    #[fail(display = "Failed to retrieve file attributes for: {}", _0)]
    GetAttr(u64),
    #[fail(display = "Failed to set file attributes for: {}", _0)]
    SetAttr(u64),
    #[fail(display = "Failed lookup of entry: {:?} in parent: {}", _1, _0)]
    Lookup(u64, OsString),
    #[fail(display = "Failed to send request to engine")]
    SendFailed,
    #[fail(display = "Invalid reply received from engine")]
    InvalidReply,
    #[fail(display = "Could not open directory: {}", _0)]
    DirOpen(u64),
    #[fail(display = "Could not close directory: {}", _0)]
    DirClose(u64),
    #[fail(display = "Could not read directory: {}", _0)]
    DirRead(u64),
    #[fail(display = "Could not open file: {}", _0)]
    FileOpen(u64),
    #[fail(display = "Could not close file: {}", _0)]
    FileClose(u64),
    #[fail(display = "Could not read file: {}", _0)]
    FileRead(u64),
    #[fail(display = "Could not write to file: {}", _0)]
    FileWrite(u64),
    #[fail(display = "Could not create file {:?} at {}", _1, _0)]
    FileCreate(u64, OsString),
    #[fail(display = "Could not create dir {:?} at {}", _1, _0)]
    DirCreate(u64, OsString),
    #[fail(display = "Access error for: {}", _0)]
    Access(u64),
}

/// Print the error description and its underlying causes
pub fn print_error_with_causes(err: &Error) {
    error!("Error: {}", err);
    {
        let mut failure = err.cause();
        error!("caused by: {}", failure);
        while let Some(cause) = failure.cause() {
            error!("caused by: {}", cause);
            failure = cause;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print_error() {
        use failure::Error;
        let f = Error::from(EngineError::SendFailed);
        print_error_with_causes(&f);
    }
}
