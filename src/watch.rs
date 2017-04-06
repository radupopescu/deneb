use notify::{Watcher, RecommendedWatcher, RecursiveMode, RawEvent, raw_watcher};

use std::path::Path;
use std::sync::mpsc::{Receiver, channel};

use errors::*;

pub struct DirectoryWatcher {
    receiver: Receiver<RawEvent>,
    watcher: RecommendedWatcher,
}

impl DirectoryWatcher {
    pub fn new() -> Result<DirectoryWatcher> {
        // Create a channel to receive the events.
        let (tx, rx) = channel();

        Ok(DirectoryWatcher {
            receiver: rx,
            watcher: raw_watcher(tx)?,
        })
    }

    pub fn watch_path(&mut self, path: &Path) -> Result<()> {
        // Add a path to be watched. All files and directories at that path and
        // below will be monitored for changes.
        self.watcher.watch(path, RecursiveMode::Recursive)?;

        Ok(())
    }

    pub fn run(&self) {
        loop {
            match self.receiver.recv() {
                Ok(event) => info!("{:?}", event),
                Err(e) => info!("watch error: {:?}", e),
            }
        }
    }
}
