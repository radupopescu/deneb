use bincode::Error as BCError;
use data_encoding::DecodeError;
use futures::Canceled as FCanceled;
use log;
use log4rs;
use lmdb_rs;
use nix;
use time;
use toml;

use std::io;
use std::path::{PathBuf, StripPrefixError};
use std::sync;
use std::time::SystemTimeError;

error_chain! {
    types {}

    links {}

    foreign_links {
        BincodeError(BCError) #[doc="bincode error"];
        DataEncodingDecodeError(DecodeError) #[doc="data_encoding decode error"];
        FutureCancelled(FCanceled) #[doc="canceled future"];
        IoError(io::Error) #[doc="io error"];
        LogError(log::SetLoggerError) #[doc="log error"];
        Log4rsConfigError(log4rs::config::Errors) #[doc="log4rs config error"];
        LmdbError(lmdb_rs::MdbError) #[doc="LMDB error"];
        NixError(nix::Error) #[doc="nix error"];
        PathStripPrefixError(StripPrefixError) #[doc="path prefix strip error"];
        DurationOutOfRangeError(time::OutOfRangeError) #[doc="duration out-of-range error"];
        StdMpscRecvError(sync::mpsc::RecvError) #[doc="std::mpsc receive error"];
        SystemTimeError(SystemTimeError) #[doc="system time conversion error"];
        TimeParseError(time::ParseError) #[doc="time format parsing error"];
        TomlDeError(toml::de::Error) #[doc="toml deserialization error"];
        TomlSerError(toml::ser::Error) #[doc="toml serialization error"];
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
