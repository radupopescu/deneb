use {
    failure::{Error, Fail},
    log::error,
    nix,
    std::{ffi::OsString, path::PathBuf},
};

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
    #[fail(display = "Get error for file: {:?}", _0)]
    FileGet(PathBuf),
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
    #[fail(display = "INode delete error for index: {}", _0)]
    INodeDelete(u64),
    #[fail(display = "Dir entry read error for index: {}", _0)]
    DEntryRead(u64),
    #[fail(display = "Dir entry write error for index: {}", _0)]
    DEntryWrite(u64),
    #[fail(display = "Invalid catalog version: {}", _0)]
    Version(u32),
    #[fail(display = "Could not update max index")]
    MaxIndexUpdate,
}

// Engine errors

#[derive(Debug, Fail)]
pub enum EngineError {
    #[fail(display = "Failed to retrieve file attributes for: {}", _0)]
    GetAttr(u64),
    #[fail(display = "Failed to set file attributes for: {}", _0)]
    SetAttr(u64),
    #[fail(display = "Failed lookup of entry: {:?} in parent: {}", _1, _0)]
    Lookup(u64, OsString),
    #[fail(display = "Could not send message over channel")]
    Send,
    #[fail(display = "No reply received from engine")]
    NoReply,
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
    #[fail(display = "Could not unlink entry {:?} at {}", _1, _0)]
    Unlink(u64, OsString),
    #[fail(display = "Could not remove dir {:?} at {}", _1, _0)]
    RemoveDir(u64, OsString),
    #[fail(
        display = "Could not rename entry {:?} at {} to {:?} at {}",
        _1, _0, _3, _2
    )]
    Rename(u64, OsString, u64, OsString),
    #[fail(display = "Access error for: {}", _0)]
    Access(u64),
    #[fail(display = "Workspace commit error")]
    Commit,
}

#[derive(Debug, Fail)]
pub enum WorkspaceError {
    #[fail(display = "Could not retrieve file workspace: {}", _0)]
    FileLookup(u64),
    #[fail(display = "Could not retrieve dir workspace: {}", _0)]
    DirLookup(u64),
    #[fail(display = "Could not retrieve inode workspace: {}", _0)]
    INodeLookup(u64),
}

#[derive(Debug, Fail)]
#[fail(
    display = "DirWorkspace lookup error at parent {} for entry {:?}",
    parent, name
)]
pub struct DirWorkspaceEntryLookupError {
    pub parent: u64,
    pub name: OsString,
}

/// Print the error description and its underlying causes
pub fn print_error_with_causes(err: &Error) {
    error!("Error: {}", err);
    {
        let mut failure = err.as_fail();
        error!("caused by: {}", failure);
        while let Some(cause) = failure.cause() {
            error!("caused by: {}", cause);
            failure = cause;
        }
    }
}
