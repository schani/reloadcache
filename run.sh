#/bin/sh

set -e

memcached -I32m -m1g -d -u memcached
/app/reloadcache -decay 50 -server "$RELOADCACHE_SERVER"
