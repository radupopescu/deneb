use std::path::PathBuf;

use inode::FileType;

pub(crate) struct DirWorkspace {
    entries: Vec<DirEntry>,
}

impl DirWorkspace {
    pub(crate) fn new(entries: &[DirEntry]) -> DirWorkspace {
        DirWorkspace {
            entries: entries.to_vec(),
        }
    }

    pub(crate) fn get_entries_tuple(&self) -> Vec<(PathBuf, u64, FileType)> {
        self.entries
            .iter()
            .map(|e| (e.name.clone(), e.index, e.entry_type))
            .collect::<Vec<_>>()
    }

    pub(crate) fn add_entry(&mut self, index: u64, name: PathBuf, entry_type: FileType) {
        self.entries.push(DirEntry { index, name, entry_type });
        self.entries.sort_by_key(|de| de.index);
    }

    pub(crate) fn remove_entry(&mut self, name: PathBuf) {
        if let Some((idx, _)) = self.entries.iter().enumerate().find(|&(_, entry)| entry.name == name) {
            self.entries.remove(idx);
        }
    }
}

#[derive(Clone)]
pub(crate) struct DirEntry {
    index: u64,
    name: PathBuf,
    entry_type: FileType,
}

impl DirEntry {
    pub(crate) fn new(index: u64, name: PathBuf, entry_type: FileType) -> DirEntry {
        DirEntry {
            index,
            name,
            entry_type,
        }
    }
}
