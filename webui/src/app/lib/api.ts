/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import axios, { AxiosError } from "axios";

const apiPrefix = "/api/v1";
const apiHost = `${apiPrefix}`;

export interface Cluster {
    name: string;
    version: number;
    shards: {};
}

// Helper methods to handle common response patterns
function handleCreateResponse(responseData: any): string {
    if (responseData?.data != undefined) {
        return "";
    } else {
        return handleError(responseData);
    }
}

function handleDeleteResponse(responseData: any): string {
    if (responseData == null || responseData.data == null || responseData.data === "ok") {
        return "";
    } else {
        return handleError(responseData);
    }
}

export async function fetchNamespaces(): Promise<string[]> {
    try {
        const { data: responseData } = await axios.get(`${apiHost}/namespaces`);
        return responseData.data.namespaces || [];
    } catch (error) {
        handleError(error);
        return [];
    }
}

export async function createNamespace(name: string): Promise<string> {
    try {
        const { data: responseData } = await axios.post(`${apiHost}/namespaces`, {
            namespace: name,
        });
        return handleCreateResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function deleteNamespace(name: string): Promise<string> {
    try {
        const { data: responseData } = await axios.delete(`${apiHost}/namespaces/${name}`);
        return handleDeleteResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function createCluster(
    name: string,
    nodes: string[],
    replicas: number,
    password: string,
    namespace: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.post(
            `${apiHost}/namespaces/${namespace}/clusters`,
            {
                name,
                nodes,
                replicas,
                password,
            }
        );
        return handleCreateResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function fetchClusters(namespace: string): Promise<string[]> {
    try {
        const { data: responseData } = await axios.get(
            `${apiHost}/namespaces/${namespace}/clusters`
        );
        return responseData.data.clusters || [];
    } catch (error) {
        handleError(error);
        return [];
    }
}

export async function fetchCluster(namespace: string, cluster: string): Promise<Object> {
    try {
        const { data: responseData } = await axios.get(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}`
        );
        return responseData.data.cluster;
    } catch (error) {
        handleError(error);
        return {};
    }
}

export async function deleteCluster(namespace: string, cluster: string): Promise<string> {
    try {
        const { data: responseData } = await axios.delete(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}`
        );
        return handleDeleteResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function importCluster(
    namespace: string,
    cluster: string,
    nodes: string[],
    password: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.post(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/import`,
            { nodes, password }
        );
        console.log("importCluster response", responseData);
        return handleCreateResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function migrateSlot(
    namespace: string,
    cluster: string,
    target: number,
    slot: string,
    slotOnly: boolean
): Promise<string> {
    try {
        const { data: responseData } = await axios.post(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/migrate`,
            {
                target: target,
                slot: slot,
                slot_only: slotOnly,
            }
        );
        return handleCreateResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function createShard(
    namespace: string,
    cluster: string,
    nodes: string[],
    password: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.post(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards`,
            { nodes, password }
        );
        return handleCreateResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function fetchShard(
    namespace: string,
    cluster: string,
    shard: string
): Promise<Object> {
    try {
        const { data: responseData } = await axios.get(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards/${shard}`
        );
        return responseData.data.shard;
    } catch (error) {
        handleError(error);
        return {};
    }
}

export async function listShards(namespace: string, cluster: string): Promise<Object[]> {
    try {
        const { data: responseData } = await axios.get(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards`
        );
        return responseData.data.shards || [];
    } catch (error) {
        handleError(error);
        return [];
    }
}

export async function deleteShard(
    namespace: string,
    cluster: string,
    shard: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.delete(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards/${shard}`
        );
        return handleDeleteResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function createNode(
    namespace: string,
    cluster: string,
    shard: string,
    addr: string,
    role: string,
    password: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.post(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes`,
            { addr, role, password }
        );
        return handleCreateResponse(responseData);
    } catch (error) {
        return handleError(error);
    }
}

export async function listNodes(
    namespace: string,
    cluster: string,
    shard: string
): Promise<Object[]> {
    try {
        const { data: responseData } = await axios.get(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes`
        );
        return responseData.data.nodes || [];
    } catch (error) {
        handleError(error);
        return [];
    }
}

export async function deleteNode(
    namespace: string,
    cluster: string,
    shard: string,
    nodeId: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.delete(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes/${nodeId}`
        );
        return handleDeleteResponse(responseData);
    } catch (error) {
        console.log(error);
        return handleError(error);
    }
}

export async function failoverShard(
    namespace: string,
    cluster: string,
    shard: string,
    preferredNodeId?: string
): Promise<{ newMasterId?: string; error?: string }> {
    try {
        const requestBody: { preferred_node_id?: string } = {};
        if (preferredNodeId) {
            requestBody.preferred_node_id = preferredNodeId;
        }

        const { data: responseData } = await axios.post(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/failover`,
            requestBody
        );

        if (responseData?.data?.new_master_id) {
            return { newMasterId: responseData.data.new_master_id };
        } else {
            return { error: handleError(responseData) };
        }
    } catch (error) {
        return { error: handleError(error) };
    }
}

function handleError(error: any): string {
    let message: string = "";
    if (error instanceof AxiosError) {
        message = error.response?.data?.error?.message || error.message;
    } else if (error instanceof Error) {
        message = error.message;
    } else if (typeof error === "object") {
        message = error?.error?.message || error?.message;
    }
    return message || "Unknown error";
}
