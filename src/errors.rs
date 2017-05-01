use log;
use log4rs;
use nix;
use time::OutOfRangeError;

use std::io;
use std::time;

error_chain! {
    types {}

    links {}

    foreign_links {
        IoError(io::Error) #[doc="io error"];
        LogError(log::SetLoggerError) #[doc="log error"];
        Log4rsConfigError(log4rs::config::Errors) #[doc="log4rs config error"];
        NixError(nix::Error) #[doc="nix error"];
        DurationOutOfRangeError(OutOfRangeError) #[doc="duration out-of-range error"];
        SystemTimeError(time::SystemTimeError) #[doc="system time conversion error"];
    }

    errors {
        CommandLineParameter(p: String) {
            description("Command-line parameter error")
            display("Command-line parameter error: '{}'", p)
        }
    }
}

