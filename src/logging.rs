use log::LogLevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::encode::json::JsonEncoder;
use log4rs::config::{Appender, Config, Root};

use errors::*;

pub fn init() -> Result<()> {
    let json_encoder = Box::new(JsonEncoder::new());
    let stdout = ConsoleAppender::builder().encoder(json_encoder).build();

    let config = Config::builder().appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder()
               .appender("stdout")
               // Just enable all logging levels for now
               .build(LogLevelFilter::Trace))?;

    let _ = ::log4rs::init_config(config)?;
    Ok(())
}
