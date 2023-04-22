#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e
BUILDER_IMAGE=${1:-none}
if test -z "$TARGET_OS"; then
    uname_S=`uname -s`
    case "$uname_S" in
        Darwin)
            TARGET_OS=darwin
            ;;
        OpenBSD)
            TARGET_OS=openbsd
            ;;
        DragonFly)
            TARGET_OS=dragonfly
            ;;
        FreeBSD)
            TARGET_OS=freebsd
            ;;
        NetBSD)
            TARGET_OS=netbsd
            ;;
        SunOS)
            TARGET_OS=solaris
            ;;
        *)
            TARGET_OS=linux
            ;;
    esac
fi

if test -z "$TARGET_ARCH"; then
    uname_M=`uname -m`
    case "$uname_M" in
        x86_64)
            TARGET_ARCH=amd64
            ;;
        arm32)
            TARGET_ARCH=arm32
            ;;
        arm64)
            TARGET_ARCH=arm64
            ;;
        ppc64*)
            TARGET_ARCH=ppc64
            ;;
        aarch64)
            TARGET_ARCH=arm
            ;;
        i386)
            TARGET_ARCH=386
            ;;
        *)
            TARGET_ARCH=amd64
            ;;
    esac
fi

GO_PROJECT=github.com/KvrocksLabs/kvrocks_controller
BUILD_DIR=./_build
VERSION=`grep "^VERSION" Changelog | head -1 | cut -d " " -f2`
BUILD_DATE=`date -u +'%Y-%m-%dT%H:%M:%SZ'`
GIT_REVISION=`git rev-parse --short HEAD`

SERVER_TARGET_NAME=kvctl-server
if [[ "$BUILDER_IMAGE" == "none" ]]; then
    GOOS="$TARGET_OS" GOARCH="$TARGET_ARCH" CGO_ENABLED=0 go build -v -ldflags \
        "-X $GO_PROJECT/version.Version=$VERSION -X $GO_PROJECT/version.BuildDate=$BUILD_DATE -X $GO_PROJECT/version.BuildCommit=$GIT_REVISION" \
        -o ${SERVER_TARGET_NAME} ${GO_PROJECT}/cmd/server
else
    docker run --rm --privileged -it -v $(pwd):/kvctl-server -w /kvctl-server \
        -e GOOS="$TARGET_OS" -e GOARCH="$TARGET_ARCH" -e CGO_ENABLED=0 \
        $BUILDER_IMAGE go build -v -ldflags \
        "-X $GO_PROJECT/version.Version=$VERSION -X $GO_PROJECT/version.BuildDate=$BUILD_DATE -X $GO_PROJECT/version.BuildCommit=$GIT_REVISION" \
        -o /kvctl-server/${SERVER_TARGET_NAME} ${GO_PROJECT}/cmd/server
fi
if [[ $? -ne 0 ]]; then
    echo "Failed to build $SERVER_TARGET_NAME"
    exit 1
fi
echo "Build $SERVER_TARGET_NAME, OS is $TARGET_OS, Arch is $TARGET_ARCH"

CLIENT_TARGET_NAME=kvctl
if [[ "$BUILDER_IMAGE" == "none" ]]; then
    GOOS="$TARGET_OS" GOARCH="$TARGET_ARCH" CGO_ENABLED=0 go build -v -ldflags \
        "-X $GO_PROJECT/version.Version=$VERSION -X $GO_PROJECT/version.BuildDate=$BUILD_DATE -X $GO_PROJECT/version.BuildCommit=$GIT_REVISION" \
        -o ${CLIENT_TARGET_NAME} ${GO_PROJECT}/cmd/client
else
    docker run --rm --privileged -it -v $(pwd):/kvctl -w /kvctl \
        -e GOOS="$TARGET_OS" -e GOARCH="$TARGET_ARCH" -e CGO_ENABLED=0 \
        $BUILDER_IMAGE go build -v -ldflags \
        "-X $GO_PROJECT/version.Version=$VERSION -X $GO_PROJECT/version.BuildDate=$BUILD_DATE -X $GO_PROJECT/version.BuildCommit=$GIT_REVISION" \
        -o /kvctl/${CLIENT_TARGET_NAME} ${GO_PROJECT}/cmd/client
fi

if [[ $? -ne 0 ]]; then
    echo "Failed to build CLIENT_TARGET_NAME"
    exit 1
fi

echo "Build $CLIENT_TARGET_NAME, OS is $TARGET_OS, Arch is $TARGET_ARCH"

rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}
mv $SERVER_TARGET_NAME ${BUILD_DIR}
mv $CLIENT_TARGET_NAME ${BUILD_DIR}
