#!/bin/bash

set -e

env GOOS=linux GOARCH=amd64 go build -o reloadcache *.go
docker build -t reloadcache .
rm reloadcache

docker tag reloadcache 633007691302.dkr.ecr.us-east-1.amazonaws.com/reloadcache:latest
docker push 633007691302.dkr.ecr.us-east-1.amazonaws.com/reloadcache:latest
