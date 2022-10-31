# Kvrocks Cluster Controller

[![Build Status](https://github.com/KvrocksLabs/kvrocks_controller/workflows/CI%20Actions/badge.svg)](https://github.com/KvrocksLabs/kvrocks_controller/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/KvrocksLabs/kvrocks_controller)](https://goreportcard.com/report/github.com/KvrocksLabs/kvrocks_controller) [![codecov](https://codecov.io/gh/KvrocksLabs/kvrocks_controller/branch/develop/graph/badge.svg?token=EKU6KU5IWK)](https://codecov.io/gh/KvrocksLabs/kvrocks_controller)

Controller for the [Kvrocks](https://github.com/apache/incubator-kvrocks#---) cluster has the following key features: 

* Failover - controller will failover or remove the master/slave node when probing failed
* Scale out the cluster in one line command
* Manage many clusters in one controller cluster
* Support multi metadata storages like etcd and so on

## Build and Running

### Requirements

* Go >= 1.16

### Build binaries 

```shell
$ git clone https://github.com/KvrocksLabs/kvrocks_controller
$ cd kvrocks_controller
$ make # You can find the binary file in the `_build` dir if all goes good
```

### 1. Run the controller server 

```shell
# Use docker-compose to setup the etcd
$ make setup
# Run the controller server
$ ./_build/kvrocks-controller-server -c config/config.yaml
```
![image](docs/images/server.gif)

### 2. Use the controller client to interactive with the server 

```shell
# Create the namespace `test-ns` 
$ create namespace --namespace test-ns
# Create the cluster `test-cluster` with nodes 127.0.0.1:6666 and 127.0.0.1:6667 
$ create cluster --namespace test-ns --cluster test-cluster --nodes 127.0.0.1:6666,127.0.0.1:6667
# List shard in cluster `test-cluster`
$ list shard --namespace test-ns --cluster test-cluster
```

![image](docs/images/client.gif)
