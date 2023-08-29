
set -e -x

go test -v ./... -covermode=atomic -coverprofile=coverage.out -race -p 1