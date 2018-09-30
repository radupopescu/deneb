# Deneb - distributed directory synchronization

[![Build status](https://travis-ci.org/radupopescu/deneb.svg?branch=master)](https://travis-ci.org/radupopescu/deneb)

## Overview

Deneb is a tool for synchronizing directories across multiple computers. The main way to use Deneb is through its file system interface.

**Work-in-progress**: the project is currently in the early stages of
development, most
[functionality](https://github.com/radupopescu/deneb/blob/master/doc/design.md)
is still missing.

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

To run Deneb with the default settings:

```
$ cargo run
```

**Note:** There is basic write support available inside the mount point, but the persistence of the changes between runs hasn't yet been implemented.

## License and authorship

The contributors are listed in AUTHORS. This project uses the MPL v2 license, see LICENSE.

## Issues

To report an issue, use the [Deneb issue tracker](https://github.com/radupopescu/deneb/issues) on GitHub.


