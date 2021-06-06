# Hoardarr File System
Opinionated disk/directory pooling on Linux. 

Status: Pre-Alpha

## Description
There are several existing pieces of software to union directories on Linux e.g. MergerFS and UnionFS with the former being the most configurable.  Hoardarr is targeted at the use case where you have lots of disks and files are accessed infrequently and power saving from spinning down disks could be quite beneficial.  Union file systems typically read each disk and then return the sum of those states from each disk, that means accessing and reading each disk to see if it has that item.

 Hoardarr takes a different approach by scanning all the directories and file stats at start up and caching that information in memory which allows for disks to be put to sleep until a file is read or written to and even then only one disk would be woke as it knows where the file is located.  Note: The underlying disks are not monitored and must not be changed outside of Hoardarr mount else the cache would get out of sync.  


## Features
* Union file system
* Metadata cached in memory (Not file contents) when not open.
* Energy efficient storage - only disk where file is located will spin up if disks have spun down due to infrequently accesses.

## Installation

* Installation libfuse e.g. apt-get install libfuse2
* Extract to /usr/share/hoardarr or where required
* chmod +x /usr/share/hoardarr/hoardarr
* ln /usr/share/hoardarr/hoardarr /usr/bin/hoardarr
* Execute e.g. hoardarr /tmp/dest /tmp/src1 /tmp/src2

## Todo list
* Renaming folders with multiple disk is not fully implemented.
* Build scripts
* Multi threaded start up scan
* Unit tests
* Extended attributes
