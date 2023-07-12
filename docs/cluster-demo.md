# Create a cluster for demo in a machine
## Prepare
you must prepare these programs
- kvrocks 
- etcd 
- redis-cli

## Quick start
start etcd
```
$./etcd
```
start the kvctl-server
```
$./kvctl-server
```

use bash script *create-cluster* quickly make a 3 masters and 3 replicas cluster.
```
$KVROCKS=/usr/local/bin/kvrocks
$./create-cluster create
$./create-cluster start
```
after do that, in the current dir will have a directory *test-cluster* 6 dir, there are 6 dirctories in the test-cluser.
```
6666  6667  6668  7666	7667  7668
```
user kvctl-cli create cluster

```
$./kvctl-cli
create namespace test-ns
cd test-ns
create cluster test-cluster --nodes 127.0.0.1:7666,127.0.0.1:7766,127.0.0.1:7667,127.0.0.1:7767,127.0.0.1:7668,127.0.0.1:7768 --replicas 2
```

after do that, you will have the cluster.

```
shard#1
127.0.0.1:6667 master
127.0.0.1:7667 slave

shard#2
127.0.0.1:6668 master
127.0.0.1:7668 slave

shard#3
127.0.0.1:6669 master
127.0.0.1:6669 slave

```
if you want stop all the kvrocks node, use this command
```
$./create-cluster stop
```

## Interact with the cluster
using redis-cli connect the cluster
```
$redis-cli -c -p 6666
>cluster nodes
```
