# Node Operations Guide

This guide covers node lifecycle management in Apache Kvrocks Controller, including common operations and recommended workflows for handling node configuration changes.

## Overview

In a Kvrocks cluster managed by the controller, nodes are the fundamental units that store data and serve requests. Each cluster is organized into shards, and each shard contains one master node and zero or more replica (slave) nodes. Understanding how to properly manage nodes is essential for cluster maintenance and operations.

## Basic Node Operations

### Listing Nodes

To view all nodes in a specific shard:

```bash
# Using kvctl CLI
kvctl get cluster <cluster-name> -n <namespace>

# Using HTTP API
GET /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes
```

This will show you the node IDs, addresses, roles (master/slave), and other metadata for each node in the shard.

### Adding a Node

To add a new replica node to an existing shard:

```bash
# Using kvctl CLI
kvctl create node <ip:port> -n <namespace> -c <cluster> --shard <shard-index>

# Using HTTP API
POST /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes
{
  "addr": "127.0.0.1:6379",
  "role": "slave",
  "password": ""
}
```

> [!NOTE]
> New nodes are always added as replicas (slaves). To promote a replica to master, use the failover operation.

### Removing a Node

To remove a node from a shard:

```bash
# Using kvctl CLI
kvctl delete node <node-id> -n <namespace> -c <cluster> --shard <shard-index>

# Using HTTP API
DELETE /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes/{node-id}
```

> [!WARNING]
> You cannot delete a master node directly. If you need to remove a master node, you must first perform a failover to promote a replica to master, then delete the old master.

## Identifying Shard Index for a Node

Before performing node operations, you need to identify which shard contains the node. Use `kvctl get cluster` to view the cluster topology:

```bash
kvctl get cluster my-cluster -n my-namespace
```

**Example output** (simplified for clarity):

```json
{
  "cluster": {
    "name": "my-cluster",
    "version": 42,
    "shards": [
      {                                    // <-- Shard index 0
        "nodes": [
          {
            "id": "K7x2mPqw9LnR5vTgH3dF8cYj",
            "addr": "10.0.1.50:6379",
            "role": "master"
          },
          {
            "id": "N4bZ9wXe2QsP6kVr8MfL1tCg",
            "addr": "10.0.1.51:6379",
            "role": "slave"
          }
        ],
        "slot_ranges": ["0-8191"]
      },
      {                                    // <-- Shard index 1
        "nodes": [
          {
            "id": "T9jW3xBn5HkM7pQr2VfC4YzL",
            "addr": "10.0.2.50:6379",
            "role": "master"
          }
        ],
        "slot_ranges": ["8192-16383"]
      }
    ]
  }
}
```

**To find the shard index**: Shards are listed in array order starting at index 0. Locate the node by its address or ID in the output, then use the array position as the shard index in subsequent commands.

## Handling Node IP Address Changes

### Why Direct IP Updates Are Not Supported

The Kvrocks Controller does not support directly updating a node's IP address or network address for the following reasons:

1. **Data Consistency**: Nodes are identified by unique IDs that are tied to their initial configuration. Changing an IP address would require complex reconciliation logic to ensure data integrity across the cluster.

2. **Cluster Topology**: The controller maintains a consistent view of cluster topology. Direct IP updates could lead to split-brain scenarios or data inconsistencies.

3. **Replication State**: Active replication relationships between master and replica nodes are tied to specific addresses. Updating an IP address would break these relationships.

Instead of direct IP updates, the controller follows a **replace-the-node** workflow that ensures data consistency and cluster integrity.

### Recommended Workflow for Replica Nodes

If a replica node's IP address changes (e.g., after a pod restart in Kubernetes):

1. **Add a new replica** at the new IP address
2. **Wait for replication** to sync data from the master
3. **Delete the old replica** node

**Example:**

```bash
# 1. Check current cluster state
kvctl get cluster my-cluster -n my-namespace

# 2. Add new replica at new IP address
kvctl create node 192.168.1.100:6379 -n my-namespace -c my-cluster --shard 0

# 3. Verify the new node appears in the cluster
kvctl get cluster my-cluster -n my-namespace
# Look for the new node in the output with role "slave"

# 4. Wait for replication sync
# The controller does not expose explicit replication lag metrics via kvctl.
# Best available signal: verify the new node is listed as a "slave" in the shard.
# For production use, consider checking replication status directly on the 
# Kvrocks master node using: redis-cli -h <master-ip> -p 6379 INFO replication

# 5. Once verified, delete the old replica node using its node ID
kvctl delete node <old-node-id> -n my-namespace -c my-cluster --shard 0
```

> [!NOTE]
> **Replication Verification Limitation**: `kvctl get cluster` shows node role but does not expose replication lag or sync status. If you need precise sync verification before deletion, connect directly to the master Kvrocks instance and run `INFO replication` to check slave offset alignment.

### Recommended Workflow for Master Nodes

If a master node's IP address changes, you must follow the **add replica → failover → delete** workflow:

1. **Add a new node** as a replica at the new IP address
2. **Wait for replication** to sync all data from the current master
3. **Perform a failover** to promote the new replica to master
4. **Delete the old master** node

**Example:**

```bash
# 1. Check current cluster state and identify the shard index
kvctl get cluster my-cluster -n my-namespace
# Note the shard index where the master needs to be replaced

# 2. Add new node as a replica at the new IP address
kvctl create node 192.168.1.100:6379 -n my-namespace -c my-cluster --shard 0

# 3. Verify the new node appears as a replica
kvctl get cluster my-cluster -n my-namespace
# Confirm the new node is listed with role "slave"

# 4. Wait for replication sync (CRITICAL STEP)
# The controller does not expose replication lag via kvctl.
# Verify sync by connecting to the master: redis-cli -h <master-ip> INFO replication
# Ensure slave offset matches master offset before proceeding.

# 5. Perform failover to promote the new replica
kvctl failover shard 0 --preferred <new-node-id> -n my-namespace -c my-cluster

# 5. Verify the failover completed successfully
kvctl get cluster my-cluster -n my-namespace
# The new node should now show as 'master'

# 6. Delete the old master node using its node ID
kvctl delete node <old-node-id> -n my-namespace -c my-cluster --shard 0
```

> [!WARNING]
> **Failover Safety - Use `--preferred` with Caution**
>
> When you specify `--preferred <node-id>`, the controller will ONLY promote that specific node. If the preferred node:
> - Has not fully synced replication data
> - Is unreachable or unhealthy  
> - Has an invalid role
>
> **The failover will FAIL**, potentially leaving your shard in a degraded state during an outage.
>
> **Safe Usage**: Only use `--preferred` after you have verified the node is fully synced (check replication offset via `INFO replication` on the master). If unsure, omit `--preferred` and let the controller automatically select the replica with the highest sequence number.

> [!IMPORTANT]
> **Old Master Node Deletion**
>
> After failover, the old master is demoted to a replica role in the controller's metadata. However:
>
> - **If the old node is unreachable** (e.g., IP changed, pod terminated), the controller cannot reconfigure it, but deletion will still succeed because the controller removes it from the cluster topology.
> - **If deletion is for an unreachable node**, this is the expected and safe workflow - the controller cleans up its metadata.
> - No force-removal is needed; standard `kvctl delete node` handles both reachable and unreachable nodes.

### Complete Example Scenario

**Scenario**: Your master node at `10.0.1.50:6379` needs to be replaced due to a pod restart that changed its IP to `10.0.1.60:6379`.

```bash
# Step 1: Check current state
$ kvctl get cluster production-cluster -n production
# Output shows shard 0 has master at 10.0.1.50:6379 with node ID: abc123xyz

# Step 2: Add new node at new IP as replica
$ kvctl create node 10.0.1.60:6379 -n production -c production-cluster --shard 0
create node: 10.0.1.60:6379 successfully.

# Step 3: Get the new node's ID and verify sync status
$ kvctl get cluster production-cluster -n production
# Output shows shard 0 now has:
#   - master: 10.0.1.50:6379 (ID: abc123xyz)
#   - slave: 10.0.1.60:6379 (ID: def456uvw)

# Step 4: Wait for replication to complete, then failover
$ kvctl failover shard 0 --preferred def456uvw -n production -c production-cluster
failover shard 0 successfully, new master id: def456uvw.

# Step 5: Verify the new topology
$ kvctl get cluster production-cluster -n production
# Output shows shard 0 now has:
#   - master: 10.0.1.60:6379 (ID: def456uvw)
#   - slave: 10.0.1.50:6379 (ID: abc123xyz)

# Step 6: Delete the old master node
$ kvctl delete node abc123xyz -n production -c production-cluster --shard 0
delete node: abc123xyz successfully.

# Step 7: Final verification
$ kvctl get cluster production-cluster -n production
# Output shows shard 0 now has only:
#   - master: 10.0.1.60:6379 (ID: def456uvw)
```

## Troubleshooting

### Issue: Cannot Delete Master Node

**Error**: The API returns an error when trying to delete a master node.

**Solution**: This is expected behavior. You must first perform a failover to demote the master to a replica, then delete it. Follow the "Recommended Workflow for Master Nodes" above.

### Issue: Failover Fails

**Possible Causes**:
1. The new replica has not fully synced with the master
2. The specified preferred node ID does not exist or is not a replica
3. Network connectivity issues between nodes

**Solution**: 
- Verify the cluster state with `kvctl get cluster`
- Ensure the new replica shows as connected and synced
- Check network connectivity between all nodes
- Try the failover again, optionally without specifying a preferred node to let the controller choose

### Issue: New Node Not Syncing

**Possible Causes**:
1. Firewall or network configuration preventing replication
2. Authentication mismatch (password mismatch)
3. The master node is unreachable

**Solution**:
- Check network connectivity between the new replica and the master
- Verify password configuration matches between nodes
- Check Kvrocks server logs on both master and replica for replication errors

## See Also

- [HTTP API Reference](API.md) - Complete API documentation
- [README](../README.md) - Getting started guide
- [Issue #276](https://github.com/apache/kvrocks-controller/issues/276) - Original discussion on node IP changes
