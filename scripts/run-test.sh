#!/bin/bash
set -e -x

go test $(go list ./...) -race -v -covermode=atomic -coverprofile=coverage.out -p 1