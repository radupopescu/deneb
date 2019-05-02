use crate::inode::{FileAttributeChanges, INode};

#[derive(Clone)]
pub(super) struct Workspace {
    inode: INode,
    pub(in crate) dirty: bool,
}

impl Workspace {
    pub(super) fn new(inode: INode, dirty: bool) -> Workspace {
        Workspace { inode, dirty }
    }

    pub(super) fn inode(&self) -> &INode {
        &self.inode
    }

    pub(super) fn update_attributes(&mut self, changes: &FileAttributeChanges) {
        self.inode.attributes.update(changes);
        self.dirty = true;
    }

    pub(super) fn update_size(&mut self, size: u64) {
        if self.inode.attributes.size != size {
            self.inode.attributes.size = size;
            self.dirty = true;
        }
    }
}
