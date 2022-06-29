# Simple Helpers to Test Filesystems

## Overview

Some very simple tools to help test filesystems. Originally written to exercise the linux FS-Cache mechanism.

The tools are:

mktestfiles
- create a bunch to test files in a range of size
- file contents are generated by math/rand source
- file name is the sha256 hash of the content

fsreadall
- reads all the files in the source
- verifies the contents using the sha256 of the content

fsreadrandom
- reads a random range within files
- runs for a specified amount f time

fsreadwrite
- selectively reads from, writes to files

## Build

Golang needs to be installed on the system.

To make the files:

    make all

