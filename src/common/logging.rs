use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config, Root};

use common::errors::LoggerError;

pub fn init(log_level: LevelFilter) -> Result<(), LoggerError> {
    let stdout = ConsoleAppender::builder().build();

    let config = Config::builder().appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder()
               .appender("stdout")
               // Just enable all logging levels for now
               .build(log_level)).map_err(|e| LoggerError::Log4rsConfig(e))?;

    if ::log4rs::init_config(config).is_err() {
        return Err(LoggerError::SetLogger);
    }

    Ok(())
}
