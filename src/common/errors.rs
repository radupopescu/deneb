use log;
use log4rs;
use lmdb_rs;
use nix;
use time::OutOfRangeError;

use std::io;
use std::path::{PathBuf,StripPrefixError};
use std::time;

error_chain! {
    types {}

    links {}

    foreign_links {
        IoError(io::Error) #[doc="io error"];
        LogError(log::SetLoggerError) #[doc="log error"];
        Log4rsConfigError(log4rs::config::Errors) #[doc="log4rs config error"];
        LmdbError(lmdb_rs::MdbError) #[doc="LMDB error"];
        NixError(nix::Error) #[doc="nix error"];
        PathStripPrefixError(StripPrefixError) #[doc="path prefix strip error"];
        DurationOutOfRangeError(OutOfRangeError) #[doc="duration out-of-range error"];
        SystemTimeError(time::SystemTimeError) #[doc="system time conversion error"];
    }

    errors {
        CommandLineParameter(p: String) {
            description("Command-line parameter error")
            display("Command-line parameter error: '{}'", p)
        }
        DirVisitError(p: PathBuf) {
            description("Recursive directory visit error")
            display("Recursive directory visit error: {:?}", p)
        }
        LmdbCatalogError(e: String) {
            description("LMDB catalog operation error")
            display("LMDB catalog operation error: {}", e)
        }
    }
}

