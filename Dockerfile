FROM golang:1.17 as build

WORKDIR /kvctl

# If you encounter some issues when pulling modules, \
# you can try to use GOPROXY, especially in China.
# ENV GOPROXY=https://goproxy.cn

COPY . .
RUN make


FROM ubuntu:focal

WORKDIR /kvctl

COPY --from=build /kvctl/_build/kvctl-server ./bin/
COPY --from=build /kvctl/_build/kvctl-client ./bin/

VOLUME /var/lib/kvctl

COPY ./LICENSE ./
COPY ./config/config.yaml /var/lib/kvctl/

EXPOSE 9379:9379
ENTRYPOINT ["./bin/kvctl-server", "-c", "/var/lib/kvctl/config.yaml"]
