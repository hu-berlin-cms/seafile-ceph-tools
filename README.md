Script to get seafile usage per library.

Try `./seafile-ceph-usage --help` for more info.

Example crontab:
```
3 3 * * * /usr/local/bin/seafile-ceph-usage -i seafile -b seafile-blocks -c seafile-commits -f seafile-fs > /home/stats/seafile_usage_`date +%s`_`date +%F`.csv
```
