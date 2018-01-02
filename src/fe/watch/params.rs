use structopt::StructOpt;

use std::path::PathBuf;

#[derive(StructOpt)]
#[structopt(about="Test app based on a file watcher")]
pub struct Params {
    #[structopt(short = "s", long = "sync_dir", parse(from_os_str))]
    pub sync_dir: PathBuf,
    #[structopt(short = "w", long = "work_dir", parse(from_os_str))]
    pub work_dir: PathBuf,
    #[structopt(long = "chunk_size", default_value = "4194304")]
    pub chunk_size: usize,
}

impl Params {
    pub fn read() -> Params {
        Params::from_args()
    }
}
