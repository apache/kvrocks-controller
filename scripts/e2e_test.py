import requests
import os

def main():
    host = "http://localhost:8080"
    print("Running end to end tests on host: ", host)

    ns  = "test-ns"


    # Create namespace
    print("Creating namespace: ", ns)
    path = "/api/v1/namespaces/"
    rsp = requests.post(host+path, json={
        "namespace":ns,
    })
    if rsp.status_code != 201 and rsp.status_code != 409:
        print("Failed to create namespace, got status code: "+ rsp.status_code)
        os.exit(1)
    print("Success to create namespace: ", ns)


    # Create Cluster
    cluster = "test-cluster"
    print("Creating cluster: ", cluster)

    nodes = ["127.0.0.1:30001","127.0.0.1:30002","127.0.0.1:30003","127.0.0.1:30004",
         "127.0.0.1:30005","127.0.0.1:30006"]
    ids = [
        "kvrockskvrockskvrockskvrockskvrocksnode1",
        "kvrockskvrockskvrockskvrockskvrocksnode2",
        "kvrockskvrockskvrockskvrockskvrocksnode3",
        "kvrockskvrockskvrockskvrockskvrocksnode4",
        "kvrockskvrockskvrockskvrockskvrocksnode5",
        "kvrockskvrockskvrockskvrockskvrocksnode6"
    ]

    init_cluster_shards = {
        "cluster": cluster,
        "shards": []
    }

    index = 0
    it = iter(nodes)
    for master, slave in zip(it, it):
        init_cluster_shards["shards"].append({
            "master": {
                "id": ids[index],
                "address": master
            },
            "slaves": [{
                "id": ids[index+1],
                "address": slave
            }]
        })
        index += 2

    path = "/api/v1/namespaces/" + ns + "/clusters"
    rsp = requests.post(host+path, json = init_cluster_shards)
    if rsp.status_code != 201 and rsp.status_code != 409:
        print("Failed to create cluster, got status code: "+ rsp.status_code)
        os.exit(1)
    print("Success to create cluster: ", cluster)

if __name__ == "__main__":
    main()