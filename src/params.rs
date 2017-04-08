use clap::{App, Arg};
use log::LogLevelFilter;

use std::path::PathBuf;

use errors::*;

pub struct AppParameters {
    pub sync_dir: PathBuf,
    pub work_dir: PathBuf,
    pub mount_point: PathBuf,
    pub log_level: LogLevelFilter,
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
                .required(true)
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
            .get_matches();

        let sync_dir = PathBuf::from(matches.value_of("sync_dir")
            .map(|d| d.to_string())
            .ok_or(ErrorKind::CommandLineParameter("sync_dir missing".to_owned()))?);
        let work_dir = PathBuf::from(matches.value_of("work_dir")
            .map(|d| d.to_string())
            .ok_or(ErrorKind::CommandLineParameter("sync_dir missing".to_owned()))?);
        let mount_point = PathBuf::from(matches.value_of("mount_point")
            .map(|d| d.to_string())
            .ok_or(ErrorKind::CommandLineParameter("mount_point missing".to_owned()))?);
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

        Ok(AppParameters {
            sync_dir: sync_dir,
            work_dir: work_dir,
            mount_point: mount_point,
            log_level: log_level,
        })
    }
}
