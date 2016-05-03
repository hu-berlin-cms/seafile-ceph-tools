# Introduction

This repo contains helper scripts for Seafile using Ceph as storage backend.

- seafile-ceph-usage (usage statistics for seafile)
- seafile-ceph2fs (Seafile objects ceph -> filesystem)


## seafile-ceph-usage

Script to get seafile usage per library.

Try `./seafile-ceph-usage --help` for more info.

Example crontab:
```
3 3 * * * /usr/local/bin/seafile-ceph-usage -i seafile -b seafile-blocks -c seafile-commits -f seafile-fs > /home/stats/seafile_usage_`date +%s`_`date +%F`.csv
```

## seafile-ceph2fs

Copy Seafile objects from ceph backend to filesystem backend. This could be helpful for making backups.

For this to work without an external source for the namespaces, at least [Ceph 0.88 is needed][1]. So you **need Ceph Hammer (0.94)**, if you run stable releases. Support for external namespace sources isn't implemented, yet.


## seafile-ceph-empty

Helper script for TESTING only. DELETES ALL OBJECTS in a pool.

[1](http://tracker.ceph.com/issues/9031)
