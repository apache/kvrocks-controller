# Kvrocks Cluster Controller

[![Build Status](https://github.com/KvrocksLabs/kvrocks_controller/workflows/CI%20Actions/badge.svg)](https://github.com/KvrocksLabs/kvrocks_controller/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/KvrocksLabs/kvrocks_controller)](https://goreportcard.com/report/github.com/KvrocksLabs/kvrocks_controller) [![codecov](https://codecov.io/gh/KvrocksLabs/kvrocks_controller/branch/develop/graph/badge.svg?token=EKU6KU5IWK)](https://codecov.io/gh/KvrocksLabs/kvrocks_controller)

Controller for the [Kvrocks](https://github.com/apache/incubator-kvrocks#---) cluster has the following key features: 

* Failover - controller will failover or remove the master/slave node when probing failed
* Scale out the cluster in one line command
* Manage many clusters in one controller cluster
* Support multi metadata storages like etcd and so on

## Build and Running

### Requirements

* go 1.16

### Build the server and client

```shell
$ git clone https://github.com/KvrocksLabs/kvrocks_controller
$ cd ./kvrocks_controller
$ make # You can find the binary file in the `_build` dir if all goes good
```

### Running the server 

```
./_build/kvrocks-controller-server -c ./config/config.yaml
```
