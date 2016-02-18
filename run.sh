#/bin/sh

set -e

memcached -I32m -m1g -d -u memcached
/app/reloadcache -server "$RELOADCACHE_SERVER"
