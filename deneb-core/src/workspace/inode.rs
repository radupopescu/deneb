use crate::inode::{FileAttributeChanges, INode};

#[derive(Clone)]
pub(in super) struct Workspace {
    inode: INode,
    pub(in crate) dirty: bool,
}

impl Workspace {
    pub(in super) fn new(inode: INode, dirty: bool) -> Workspace {
        Workspace{ inode, dirty }
    }

    pub(in super) fn inode(&self) -> &INode {
        &self.inode
    }

    pub(in super) fn update_attributes(&mut self, changes: &FileAttributeChanges) {
        self.inode.attributes.update(changes);
        self.dirty = true;
    }

    pub(in super) fn update_size(&mut self, size: u64) {
        if self.inode.attributes.size != size {
            self.inode.attributes.size = size;
            self.dirty = true;
        }
    }
}
