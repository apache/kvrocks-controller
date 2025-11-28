# HTTP APIs

## Namespace APIs
### Create Namespace
```shell
POST /api/v1/namespaces
```
#### Request Body
```json
{ "namespace": "test-ns" }
```
#### Response JSON Body
* 201
```json
{ "data": "created" }
```
* 409
```json
{ "error": { "message": "the entry already existed" } }
```
* 5XX
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### List Namespace
```shell
GET /api/v1/namespaces
```
#### Response JSON Body
* 200
```json
{ "data": { "namespaces": ["test-ns"] } }
```
* 5XX
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Delete Namespace
```shell
DELETE /api/v1/namespaces/{namespace}
```
#### Response JSON Body
* 200
```json
{ "data": "ok" }
```
* 404
```json
{ "error": { "message": "the entry does not exist" } }
```
* 5XX
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

---

## Cluster APIs
### Create Cluster
```shell
POST /api/v1/namespaces/{namespace}/clusters
```
#### Request Body
```json
{
  "name":"test-cluster",
  "nodes":["127.0.0.1:6666"],
  "replicas":1,
  "password":""
}
```
#### Response JSON Body
* 201
```json
{ "data": "created" }
```
* 409
```json
{ "error": { "message": "the entry already existed" } }
```
* 5XX
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### List Cluster
```shell
GET /api/v1/namespaces/{namespace}/clusters
```
#### Response JSON Body
* 200
```json
{ "data": { "clusters": ["test-cluster"] } }
```
* 5XX
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Import Cluster
```shell
POST /api/v1/namespaces/{namespace}/clusters/{cluster}/import
```
#### Request Body
```json
{ "nodes":["127.0.0.1:6666"], "password":"" }
```
#### Response
* 201
```json
{ "data": "created" }
```
* 409  
```json
{ "error": { "message": "the entry already existed" } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Get Cluster
```shell
GET /api/v1/namespaces/{namespace}/clusters/{cluster}
```
#### Response JSON Body
* 200
```json
{
  "data": {
    "cluster": {
      "name":"test-cluster",
      "version":0,
      "shards":[
        {
          "nodes":[
            {
              "id":"YotDS..",
              "addr":"127.0.0.1:6666",
              "role":"master",
              "password":"",
              "master_auth":"",
              "created_at":16834433980
            }
          ],
          "slot_ranges":["0-16383"],
          "import_slot":-1,
          "migrating_slot":-1
        }
      ]
    }
  }
}
```
* 404  
```json
{ "error": { "message": "the entry does not exist" } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Delete Cluster
```shell
DELETE /api/v1/namespaces/{namespace}/clusters/{cluster}
```
#### Response JSON Body
* 200
```json
{ "data": "ok" }
```
* 404
```json
{ "error": { "message": "the entry does not exist" } }
```
* 5XX
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

---

## Shard APIs
### Create Shard
```shell
POST /api/v1/namespaces/{namespace}/clusters/{cluster}/shards
```
#### Request Body
```json
{ "nodes":["127.0.0.1:6666"], "password":"" }
```
#### Response JSON Body
* 201
```json
{ "data": "created" }
```
* 409
```json
{ "error": { "message": "the entry already existed" } }
```
* 5XX
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Get Shard
```shell
GET /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}
```
#### Response JSON Body
* 200
```json
{
  "data": {
    "shard": {
      "nodes":[{ "id":"3SSt...", "addr":"127.0.0.1:6666", "role":"master" }],
      "slot_ranges":["0-16383"],
      "import_slot":-1,
      "migrating_slot":-1
    }
  }
}
```
* 404  
```json
{ "error": { "message": "the entry does not exist" } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### List Shards
```shell
GET /api/v1/namespaces/{namespace}/clusters/{cluster}/shards
```
* 200  
```json
{ "data": { "shards": [...] } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Delete Shard
```shell
DELETE /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}
```
* 200  
```json
{ "data": "ok" }
```
* 404  
```json
{ "error": { "message": "the entry does not exist" } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Failover master node
```shell
POST /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/failover
```
#### Request Body
```json
{ "preferred_node_id": "{NODE_ID}" }
```
#### Response
```json
{ "data": { "new_master_id": "{NEW_MASTER_ID}" } }
```

---

## Node APIs

### Create Node
```shell
POST /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes
```
#### Request Body
```json
{
  "addr": "127.0.0.1:6666",
  "role": "slave",
  "password":""
}
```
#### Response
* 201  
```json
{ "data": "created" }
```
* 409  
```json
{ "error": { "message": "the entry already existed" } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### List Nodes
```shell
GET /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes
```
* 200  
```json
{ "data": { "nodes": [...] } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

### Delete Node
```shell
DELETE /api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes/{nodeID}
```
* 200  
```json
{ "data": "ok" }
```
* 404  
```json
{ "error": { "message": "the entry does not exist" } }
```
* 5XX  
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

---

# 🚀 Migration APIs (UPDATED)

## Migration APIs

Slot migration moves a specific slot from one shard to another.  
Used for resharding, scaling, and load balancing.

---

### Migrate Slot
```shell
POST /api/v1/namespaces/{namespace}/clusters/{cluster}/migrate
```

#### Description
This API initiates slot migration from source → target shard.

#### Request Body
```json
{
  "target": 1,
  "slot": 123,
  "slot_only": false,
  "pipeline_size": 128,
  "speed": 4096
}
```

#### Field Description

| Field | Type | Required | Description |
|-------|-------|----------|-------------|
| target | integer | Yes | Target shard ID |
| slot | integer | Yes | Slot number |
| slot_only | boolean | No | Metadata only |
| pipeline_size | integer | No | Keys per batch |
| speed | integer | No | Max speed, 0 = unlimited |

---

### Responses

#### ✔ 200 OK
```json
{ "data": "ok" }
```

#### ❌ 400 Bad Request
```json
{ "error": { "message": "invalid slot number" } }
```

#### ❌ 404 Not Found
```json
{ "error": { "message": "the entry does not exist" } }
```

#### ❌ 409 Conflict
```json
{ "error": { "message": "slot is already migrating" } }
```

#### ❌ 429 Controller Busy
```json
{ "error": { "message": "controller is handling another migration" } }
```

#### ❌ 5XX Internal Error
```json
{ "error": { "message": "DETAIL ERROR STRING" } }
```

---

### Slot State Flow

| State | Meaning |
|--------|---------|
| stable | normal |
| migrating | keys moving |
| importing | target receiving |
| stable | migration done |

---

### Notes
- Only **1 migration at a time** per cluster.  
- `slot_only=true` → no key copying.  
- Progress can be checked using **Get Shard API**.

