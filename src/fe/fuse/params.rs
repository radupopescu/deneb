use clap::{App, Arg};
use log::LogLevelFilter;

use std::path::PathBuf;

use common::errors::*;

pub const DEFAULT_CHUNK_SIZE: u64 = 4194304; // 4MB default

pub struct AppParameters {
    pub work_dir: PathBuf,
    pub mount_point: PathBuf,
    pub log_level: LogLevelFilter,
    pub chunk_size: u64,
    pub sync_dir: Option<PathBuf>,
}

impl AppParameters {
    pub fn read() -> Result<AppParameters> {
        let matches = App::new("Deneb")
            .version("0.1.0")
            .author("Radu Popescu <mail@radupopescu.net>")
            .about("Flew into the light of Deneb")
            .arg(Arg::with_name("sync_dir")
                .short("s")
                .long("sync_dir")
                .takes_value(true)
                .value_name("SYNC_DIR")
                .required(false)
                .help("Synced directory"))
            .arg(Arg::with_name("work_dir")
                .short("w")
                .long("work_dir")
                .takes_value(true)
                .value_name("WORK_DIR")
                .required(true)
                .help("Work (scratch) directory"))
            .arg(Arg::with_name("mount_point")
                .short("m")
                .long("mount_point")
                .takes_value(true)
                .value_name("MOUNT_POINT")
                .required(true)
                .help("Mount point"))
            .arg(Arg::with_name("log_level")
                .short("l")
                .long("log_level")
                .takes_value(true)
                .value_name("LOG_LEVEL")
                .required(false)
                .default_value("info")
                .help("Log level for the console logger"))
            .arg(Arg::with_name("chunk_size")
                 .long("chunk_size")
                 .takes_value(true)
                 .value_name("CHUNK_SIZE")
                 .required(false)
                 .default_value("DEFAULT")//DEFAULT_CHUNK_SIZE) // default 4MB chunks
                 .help("Chunk size used for storing files"))
            .get_matches();

        let sync_dir = matches.value_of("sync_dir").map(PathBuf::from);
        let work_dir = PathBuf::from(matches.value_of("work_dir")
            .map(|d| d.to_string())
            .ok_or_else(|| ErrorKind::CommandLineParameter("sync_dir missing".to_owned()))?);
        let mount_point = PathBuf::from(matches.value_of("mount_point")
            .map(|d| d.to_string())
            .ok_or_else(|| ErrorKind::CommandLineParameter("mount_point missing".to_owned()))?);
        let log_level = match matches.value_of("log_level") {
            Some("off") => LogLevelFilter::Off,
            Some("error") => LogLevelFilter::Error,
            Some("warn") => LogLevelFilter::Warn,
            Some("info") => LogLevelFilter::Info,
            Some("debug") => LogLevelFilter::Debug,
            Some("trace") => LogLevelFilter::Trace,
            Some(level) => {
                bail!(ErrorKind::CommandLineParameter("invalid log_level: ".to_string() + level))
            },
            None => { LogLevelFilter::Info }
        };
        let chunk_size = match matches.value_of("chunk_size") {
            Some("DEFAULT") | None => {
                DEFAULT_CHUNK_SIZE
            }
            Some(chunk_size) => {
                match u64::from_str_radix(chunk_size, 10) {
                    Ok(size) => {
                        size
                    }
                    _ => {
                        DEFAULT_CHUNK_SIZE
                    }
                }
            }
        };

        Ok(AppParameters {
            work_dir: work_dir,
            mount_point: mount_point,
            log_level: log_level,
            chunk_size: chunk_size,
            sync_dir: sync_dir,
        })
    }
}
