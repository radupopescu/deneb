use log::LevelFilter;
use structopt::StructOpt;

use std::path::PathBuf;

use deneb_common::errors::DenebError;

fn parse_log_level_str(s: &str) -> Result<LevelFilter, DenebError> {
    match s {
        "off" => Ok(LevelFilter::Off),
        "error" => Ok(LevelFilter::Error),
        "warn" => Ok(LevelFilter::Warn),
        "info" => Ok(LevelFilter::Info),
        "debug" => Ok(LevelFilter::Debug),
        "trace" => Ok(LevelFilter::Trace),
        _ => Err(DenebError::CommandLineParameter(
            "log_level: ".to_string() + s,
        )),
    }
}

#[derive(StructOpt)]
#[structopt(about = "Flew into the light of Deneb")]
pub struct AppParameters {
    #[structopt(short = "w", long = "work_dir", parse(from_os_str))] pub work_dir: PathBuf,
    #[structopt(short = "m", long = "mount_point", parse(from_os_str))] pub mount_point: PathBuf,
    #[structopt(short = "l", long = "log_level", default_value = "info",
                parse(try_from_str = "parse_log_level_str"))]
    pub log_level: LevelFilter,
    // Default chunk size: 4 MB
    #[structopt(long = "chunk_size", default_value = "4194304")] pub chunk_size: usize,
    #[structopt(short = "s", long = "sync_dir", parse(from_os_str))] pub sync_dir: Option<PathBuf>,
}

impl AppParameters {
    pub fn read() -> AppParameters {
        AppParameters::from_args()
    }
}
