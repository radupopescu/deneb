use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config, Root};

use common::errors::*;

pub fn init(log_level: LevelFilter) -> Result<()> {
    let stdout = ConsoleAppender::builder().build();

    let config = Config::builder().appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder()
               .appender("stdout")
               // Just enable all logging levels for now
               .build(log_level))?;

    let _ = ::log4rs::init_config(config)?;
    Ok(())
}
