#!/bin/bash
set -e -x

docker-compose -f ./scripts/docker-compose.yml up -d

go test -v ./... -covermode=atomic -coverprofile=coverage.out -race -p 1