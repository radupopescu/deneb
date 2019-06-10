# Design overview

Deneb is a solution for synchronizing directories across multiple computers. File contents are efficiently stored as immutable content-addressed blobs. File metadata is held in a catalog file, which represents a Merkle tree encoding of the entire directory tree state at a given point in time.

This representation of file data and metadata is chosen to facilitate the distribution and synchronization of the directory tree on multiple computers.

Deneb offers a file system interface to the contents of the synchronized directory tree. The planned feature set, to distinguish it from existing solutions is:

* Immutable content-addressed storage - old versions of files are not deleted, since content blocks are never modified; ability to revert to an earlier state of the synchronized directory.
* Deduplication - comes for free from the use of content-addressed storage.
* Compression - content chunks should be stored compressed to reduce space requirements and the amount of data to be transfered.
* End-to-end encryption - data should never leave the clients unencrypted.
* (Optional) Laziness - file contents are only transfered between clients when needed.
* (Optional) Decentralized - it may be possible to do synchronization with a peer-to-peer approach, instead of using a central server.
* Open Source (MPLv2) - it's best to be able to inspect the code that is storing your data, moving it around, encrypting it etc.
