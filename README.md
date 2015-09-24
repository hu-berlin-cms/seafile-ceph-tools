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
