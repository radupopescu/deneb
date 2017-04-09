use std::fmt;
use std::fs::{DirEntry, FileType, Permissions, read_dir};
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::time::{UNIX_EPOCH, SystemTime};

use chrono::datetime::DateTime;
use chrono::offset::utc::UTC;
use chrono::naive::datetime::NaiveDateTime;
use nix::sys::stat::stat;

use errors::*;

struct Item<H> {
    name: PathBuf,
    uid: u32,
    gid: u32,
    file_type: FileType,
    permissions: Permissions,
    creation_time: Option<SystemTime>,
    access_time: Option<SystemTime>,
    modification_time: Option<SystemTime>,
    content_hash: H,
}

fn system_to_date_time(st: &Option<SystemTime>) -> Result<DateTime<UTC>> {
    if let Some(st) = *st {
        let duration = ::chrono::Duration::from_std(st.duration_since(UNIX_EPOCH)?)?;
        let unix = NaiveDateTime::from_timestamp(0, 0);
        let final_time = unix + duration;
        Ok(DateTime::<UTC>::from_utc(final_time, UTC))
    } else {
        bail!("No system time given")
    }
}

impl<H> fmt::Display for Item<H> where H: fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (system_to_date_time(&self.creation_time),
               system_to_date_time(&self.access_time),
               system_to_date_time(&self.modification_time)) {
            (Ok(ct), Ok(at), Ok(mt)) => {
                write!(f,
                       "Path: {:?}, uid: {}, gid: {}, file_type: {:?}, permissions: {:?}, \
                        creation_time: {}, access_time: {}, modification_time: {}, content_hash: \
                        {:?}",
                       self.name,
                       self.uid,
                       self.gid,
                       self.file_type,
                       self.permissions,
                       ct,
                       at,
                       mt,
                       self.content_hash)
            }
            _ => {
                write!(f,
                       "Path: {:?}, uid: {}, gid: {}, file_type: {:?}, permissions: {:?}, \
                        content_hash: {:?}",
                       self.name,
                       self.uid,
                       self.gid,
                       self.file_type,
                       self.permissions,
                       self.content_hash)
            }
        }
    }
}

pub struct Catalog<H> {
    items: Vec<Item<H>>,
}

impl<H> Catalog<H> where H: Hash + Default {
    pub fn new() -> Catalog<H> {
        Catalog { items: Vec::new() }
    }

    pub fn from_dir(dir: &Path) -> Result<Catalog<H>> {
        let mut catalog = Catalog::new();
        let _ = visit_dirs(dir, &mut |e| catalog.add_item(e))?;
        Ok(catalog)
    }

    pub fn add_item(&mut self, entry: &DirEntry) -> Result<()> {
        let stats = stat(entry.path().as_path())?;
        let metadata = entry.metadata()?;
        self.items.push(Item {
            name: entry.path(),
            uid: stats.st_uid,
            gid: stats.st_gid,
            file_type: metadata.file_type(),
            permissions: metadata.permissions(),
            creation_time: metadata.created().ok(),
            access_time: metadata.accessed().ok(),
            modification_time: metadata.modified().ok(),
            content_hash: H::default(),
        });
        Ok(())
    }

    pub fn show(&self) {
        if self.items.len() > 0 {
            info!("Catalog contents:");
            for i in &self.items {
                info!("Name: {:?}", i.name);
            }
        } else {
            info!("Catalog empty.");
        }
    }
}

fn visit_dirs(dir: &Path, cb: &mut FnMut(&DirEntry) -> Result<()>) -> Result<()> {
    if dir.is_dir() {
        for entry in read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            cb(&entry)?;
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_time_to_date_time() {
        let st = Some(SystemTime::now());
        println!("{}", system_to_date_time(&st).unwrap());
    }
}
