#!/bin/bash
set -e -x

docker pull quay.io/coreos/etcd
docker-compose -f ./scripts/etcd-compose.yml up -d

go test -v ./... -covermode=atomic -coverprofile=coverage.out -race -p 1