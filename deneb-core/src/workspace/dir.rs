use {
    crate::inode::FileType,
    std::path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub(super) struct Workspace {
    entries: Vec<DirEntry>,
    pub(in crate) dirty: bool,
}

impl Workspace {
    pub(super) fn new(entries: &[DirEntry]) -> Workspace {
        Workspace {
            entries: entries.to_vec(),
            dirty: false,
        }
    }

    pub(super) fn entries(&self) -> &[DirEntry] {
        &self.entries
    }

    pub(super) fn entries_tuple(&self) -> Vec<(PathBuf, u64, FileType)> {
        self.entries
            .iter()
            .map(|e| (e.name.clone(), e.index, e.entry_type))
            .collect::<Vec<_>>()
    }

    pub(super) fn entry_index(&self, name: &Path) -> Option<u64> {
        self.entries
            .iter()
            .find(|&entry| entry.name == name)
            .map(|entry| entry.index as u64)
    }

    pub(super) fn entry(&self, name: &Path) -> Option<&DirEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    pub(super) fn add_entry(&mut self, index: u64, name: PathBuf, entry_type: FileType) {
        self.entries.push(DirEntry {
            index,
            name,
            entry_type,
        });
        self.entries.sort_by_key(|de| de.index);

        self.dirty = true;
    }

    pub(super) fn remove_entry(&mut self, name: &Path) {
        if let Some(idx) = self
            .entries
            .iter()
            .enumerate()
            .find(
                |&(
                    _,
                    &DirEntry {
                        name: ref ename, ..
                    },
                )| ename == name,
            )
            .map(|(idx, _)| idx)
        {
            self.entries.remove(idx as usize);
        }

        self.dirty = true;
    }

    pub(super) fn remove_entry_idx(&mut self, idx: u64) {
        self.entries.retain(|entry| entry.index != idx);
    }
}

#[derive(Clone, Debug)]
pub(super) struct DirEntry {
    pub(super) index: u64,
    pub(super) name: PathBuf,
    pub(super) entry_type: FileType,
}

impl DirEntry {
    pub(super) fn new(index: u64, name: PathBuf, entry_type: FileType) -> DirEntry {
        DirEntry {
            index,
            name,
            entry_type,
        }
    }
}
