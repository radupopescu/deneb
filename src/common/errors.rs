use log4rs;
use nix;

use std::path::PathBuf;

pub type DenebResult<T> = ::std::result::Result<T, ::failure::Error>;

// Errors from the nix crate

#[derive(Debug, Fail)]
#[fail(display = "Nix error: {}", inner)]
pub struct UnixError {
    #[cause] inner: nix::Error,
}

impl From<nix::Error> for UnixError {
    fn from(ne: nix::Error) -> UnixError {
        UnixError { inner: ne }
    }
}

// Errors related to the logger

#[derive(Debug, Fail)]
pub enum LoggerError {
    #[fail(display = "Error setting logger")]
    SetLogger,
    #[fail(display = "Log4rs configuration error")]
    Log4rsConfig(#[cause] log4rs::config::Errors),
}

// Internal Deneb errors

#[derive(Debug, Fail)]
pub enum DenebError {
    #[fail(display = "Disk IO error")]
    DiskIO,
    #[fail(display = "Chunk retrieval error")]
    ChunkRetrieval,
    #[fail(display = "Command line parameter parsing error: {}", _0)]
    CommandLineParameter(String),
    #[fail(display = "Directory visit error: {:?}", _0)]
    DirectoryVisit(PathBuf),
    #[fail(display = "Index generator error")]
    IndexGenerator,
    #[fail(display = "LMDB catalog error: {}", _0)]
    LmdbCatalogError(String),
}
