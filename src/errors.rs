use log;
use log4rs;
use nix;
use notify;
use time::OutOfRangeError;

use std::io;
use std::time;

use merkle;

error_chain! {
    types {}

    links {
        Merkle(merkle::errors::Error, merkle::errors::ErrorKind);
    }

    foreign_links {
        IoError(io::Error) #[doc="io error"];
        LogError(log::SetLoggerError) #[doc="log error"];
        Log4rsConfigError(log4rs::config::Errors) #[doc="log4rs config error"];
        NixError(nix::Error) #[doc="nix error"];
        NotifyError(notify::Error) #[doc="notify error"];
        DurationOutOfRangeError(OutOfRangeError) #[doc="duration out-of-range error"];
        SystemTimeError(time::SystemTimeError) #[doc="system time conversion error"];
    }

    errors {
        MissingCommandLineParameter(p: String) {
            description("Missing command-line parameters")
            display("Missing command-line parameters: '{}'", p)
        }
    }
}

