# Deneb - distributed directory syncronization

[![Build status](https://travis-ci.org/radupopescu/deneb.svg?branch=master)](https://travis-ci.org/radupopescu/deneb)

## Overview

Deneb is a solution for syncronizing directories across multiple computers. File contents are efficiently stored as immutable content-addressed blobs. File metadata is held in a catalog file, which represents a Merkle tree encoding of the entire directory tree state at a given point in time.

This representation of file data and metadata is chosen to facilitate the distribution and synchronization of the directory tree on multiple computers.

Deneb offers a file system interface to the contents of the synchronized directory tree.

**Work-in-progress**: the project is currently in the early stages of development, most functionality is still missing. More details will follow.

## Building

The file system interface of Deneb depends on FUSE. On Linux, it's available in the distribution's package repository. For example, on Ubuntu, FUSE can be installed as follows:

```
$ sudo apt install fuse libfuse-dev
```

On macOS, there is [OSXFUSE](https://osxfuse.github.io/), which can either be installed manually or by using Homebrew Cask:

```
$ brew cask install osxfuse
```

Deneb is built as a standard Rust application using Cargo:

```
$ cargo build --all
```

To run the test suite:

```
$ cargo test --all
```

The longer property based integration tests (QuickCheck) are not run by default, but they can be run explicitly:

```
$ cargo test --all -- --ignored
```

## Running

Currently, Deneb has to be manually started from the command line. There is a convenience script for starting Deneb during development:

```
$ ./scripts/start.sh [-s] -l debug <WORKSPACE_PREFIX>
```

where `<WORKSPACE_PREFIX>` is a directory which contains:

* The internal data files of Deneb, at `<WORKSPACE_PREFIX>/internal`
* The mount point where the contents of the directory tree can be accessed, at `<WORKSPACE_PREFIX>/mount`
* If the `-s` parameter is given, any preexisting files and directories under `<WORKSPACE_PREFIX>/data` are used to prepopulate the Deneb synchronized directory.

The `internal`, `mount` and `data` subdirectories will be created by the script, if needed.

**Note:** There is basic write support available inside the mount point, but the persistence of the changes between runs hasn't yet been implemented.

## License and authorship

The contributors are listed in AUTHORS. This project uses the MPL v2 license, see LICENSE.

## Issues

To report an issue, use the [Deneb issue tracker](https://github.com/radupopescu/deneb/issues) on GitHub.


