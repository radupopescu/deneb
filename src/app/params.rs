use log::LevelFilter;
use structopt::StructOpt;

use std::path::PathBuf;

use deneb_core::errors::DenebError;

#[derive(StructOpt)]
#[structopt(about = "Flew into the light of Deneb")]
pub struct Parameters {
    #[structopt(
        short = "n",
        long = "instance_name",
        default_value = "main",
        help = "Name of the Deneb instance"
    )]
    pub instance_name: String,
    #[structopt(
        short = "c",
        long = "config_dir",
        parse(from_os_str),
        help = "Configuration directory"
    )]
    pub config_dir: Option<PathBuf>,
    #[structopt(
        short = "m",
        long = "mount_point",
        parse(from_os_str),
        help = "Location where the file system is mounted"
    )]
    pub(super) mount_point: Option<PathBuf>,
    #[structopt(
        short = "l",
        long = "log_level",
        default_value = "info",
        parse(try_from_str = "parse_log_level_str"),
        help = "Logging level (off|error|warn|info|debug|trace)"
    )]
    pub log_level: LevelFilter,
    // Default chunk size: 4 MB
    #[structopt(
        long = "chunk_size",
        default_value = "4194304",
        help = "Default chunk size for storing files"
    )]
    pub chunk_size: usize,
    #[structopt(
        short = "s",
        long = "sync_dir",
        parse(from_os_str),
        help = "Populate the repository with the contents of this directory"
    )]
    pub sync_dir: Option<PathBuf>,
    #[structopt(
        short = "f",
        long = "force_unmount",
        help = "Force unmount the file system on exit"
    )]
    pub force_unmount: bool,
    #[structopt(
        long = "foreground",
        help = "Stay in the foreground, don't fork"
    )]
    pub foreground: bool,
}

impl Parameters {
    pub fn read() -> Parameters {
        Parameters::from_args()
    }
}

#[derive(Deserialize, Serialize)]
pub(super) struct ConfigFileParameters {
    pub(super) mount_point: Option<PathBuf>,
    #[serde(default = "default_log_level")]
    pub(super) log_level: LevelFilter,
    #[serde(default = "default_chunk_size")]
    pub(super) chunk_size: usize,
}

fn default_log_level() -> LevelFilter {
    LevelFilter::Info
}

fn default_chunk_size() -> usize {
    4194304
}

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
