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

GO_PROJECT=github.com/RocksLabs/kvrocks_controller
BUILD_DIR=./_build
VERSION=`cat VERSION.txt`
BUILD_DATE=`date -u +'%Y-%m-%dT%H:%M:%SZ'`
GIT_REVISION=`git rev-parse --short HEAD`

SERVER_TARGET_NAME=kvctl-server
CLIENT_TARGET_NAME=kvctl-client

for TARGET_NAME in "$SERVER_TARGET_NAME" "$CLIENT_TARGET_NAME"; do
    CMD_PATH="${GO_PROJECT}/cmd/${TARGET_NAME##*-}" # Remove everything to the left of the last - in the TARGET_NAME variable

    if [[ "$BUILDER_IMAGE" == "none" ]]; then
        GOOS="$TARGET_OS" GOARCH="$TARGET_ARCH" CGO_ENABLED=0 go build -v -ldflags \
            "-X $GO_PROJECT/version.Version=$VERSION -X $GO_PROJECT/version.BuildDate=$BUILD_DATE -X $GO_PROJECT/version.BuildCommit=$GIT_REVISION" \
            -o ${TARGET_NAME} ${CMD_PATH}
    else
        docker run --rm --privileged -it -v $(pwd):/${TARGET_NAME} -w /${TARGET_NAME} \
            -e GOOS="$TARGET_OS" -e GOARCH="$TARGET_ARCH" -e CGO_ENABLED=0 \
            $BUILDER_IMAGE go build -v -ldflags \
            "-X $GO_PROJECT/version.Version=$VERSION -X $GO_PROJECT/version.BuildDate=$BUILD_DATE -X $GO_PROJECT/version.BuildCommit=$GIT_REVISION" \
            -o /${TARGET_NAME}/${TARGET_NAME} ${CMD_PATH}
    fi

    if [[ $? -ne 0 ]]; then
        echo "Failed to build $TARGET_NAME"
        exit 1
    fi

    echo "Build $TARGET_NAME, OS is $TARGET_OS, Arch is $TARGET_ARCH"
done

rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}
mv $SERVER_TARGET_NAME ${BUILD_DIR}
mv $CLIENT_TARGET_NAME ${BUILD_DIR}
