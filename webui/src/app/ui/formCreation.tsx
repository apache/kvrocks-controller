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

"use client";
import React from "react";
import FormDialog from "./formDialog";
import {
    createCluster,
    createNamespace,
    createNode,
    createShard,
    importCluster,
    migrateSlot,
    listShards,
} from "../lib/api";
import { useRouter } from "next/navigation";

type NamespaceFormProps = {
    position: string;
    children?: React.ReactNode;
};

type ClusterFormProps = {
    position: string;
    namespace: string;
    children?: React.ReactNode;
};

type ShardFormProps = {
    position: string;
    namespace: string;
    cluster: string;
    children?: React.ReactNode;
};

type NodeFormProps = {
    position: string;
    namespace: string;
    cluster: string;
    shard: string;
    children?: React.ReactNode;
};

const containsWhitespace = (value: string): boolean => /\s/.test(value);

const validateFormData = (formData: FormData, fields: string[]): string | null => {
    for (const field of fields) {
        const value = formData.get(field);
        if (typeof value === "string" && containsWhitespace(value)) {
            return `${
                field.charAt(0).toUpperCase() + field.slice(1)
            } cannot contain any whitespace characters.`;
        }
    }
    return null;
};

export const NamespaceCreation: React.FC<NamespaceFormProps> = ({ position, children }) => {
    const router = useRouter();
    const handleSubmit = async (formData: FormData) => {
        const fieldsToValidate = ["name"];
        const errorMessage = validateFormData(formData, fieldsToValidate);
        if (errorMessage) {
            return errorMessage;
        }

        const formObj = Object.fromEntries(formData.entries());
        const response = await createNamespace(formObj["name"] as string);
        if (response === "") {
            router.push(`/namespaces/${formObj["name"]}`);
        } else {
            return response || "Failed to create namespace";
        }
    };

    return (
        <FormDialog
            position={position}
            title="Create Namespace"
            submitButtonLabel="Create"
            formFields={[{ name: "name", label: "Input Name", type: "text", required: true }]}
            onSubmit={handleSubmit}
        />
    );
};

export const ClusterCreation: React.FC<ClusterFormProps> = ({ position, namespace, children }) => {
    const router = useRouter();
    const handleSubmit = async (formData: FormData) => {
        const fieldsToValidate = ["name", "replicas"];
        const errorMessage = validateFormData(formData, fieldsToValidate);
        if (errorMessage) {
            return errorMessage;
        }
        const formObj = Object.fromEntries(formData.entries());
        const nodes = JSON.parse(formObj["nodes"] as string) as string[];
        if (nodes.length === 0) {
            return "Nodes cannot be empty.";
        }

        for (const node of nodes) {
            if (containsWhitespace(node)) {
                return "Nodes cannot contain any whitespace characters.";
            }
        }

        const response = await createCluster(
            formObj["name"] as string,
            nodes,
            parseInt(formObj["replicas"] as string),
            formObj["password"] as string,
            namespace
        );
        if (response === "") {
            router.push(`/namespaces/${namespace}/clusters/${formObj["name"]}`);
        } else {
            return response || "Failed to create cluster";
        }
    };

    return (
        <FormDialog
            position={position}
            title="Create Cluster"
            submitButtonLabel="Create"
            formFields={[
                { name: "name", label: "Input Name", type: "text", required: true },
                { name: "nodes", label: "Input Nodes", type: "array", required: true },
                {
                    name: "replicas",
                    label: "Input Replicas",
                    type: "text",
                    required: true,
                },
                {
                    name: "password",
                    label: "Input Password",
                    type: "password",
                },
            ]}
            onSubmit={handleSubmit}
        >
            {children}
        </FormDialog>
    );
};

export const ShardCreation: React.FC<ShardFormProps> = ({
    position,
    namespace,
    cluster,
    children,
}) => {
    const router = useRouter();
    const handleSubmit = async (formData: FormData) => {
        try {
            const fieldsToValidate = ["nodes"];
            const errorMessage = validateFormData(formData, fieldsToValidate);
            if (errorMessage) {
                return errorMessage;
            }

            const formObj = Object.fromEntries(formData.entries());

            let nodes: string[];
            try {
                const nodesString = formObj["nodes"] as string;
                if (!nodesString) {
                    return "Nodes field is required.";
                }
                nodes = JSON.parse(nodesString) as string[];
            } catch (parseError) {
                return "Invalid nodes format. Please check your input.";
            }

            const password = (formObj["password"] as string) || "";

            if (!Array.isArray(nodes) || nodes.length === 0) {
                return "At least one node is required.";
            }

            for (const node of nodes) {
                if (!node || typeof node !== "string") {
                    return "All nodes must be valid address strings.";
                }
                if (containsWhitespace(node)) {
                    return "Node addresses cannot contain whitespace characters.";
                }
            }

            const response = await createShard(namespace, cluster, nodes, password);
            if (response === "") {
                // Refresh the page to show the new shard
                window.location.reload();
            } else {
                return response || "Failed to create shard";
            }
        } catch (error) {
            console.error("Error in shard creation:", error);
            return `Failed to create shard: ${error instanceof Error ? error.message : "Unknown error"}`;
        }
    };

    return (
        <FormDialog
            position={position}
            title="Create Shard"
            submitButtonLabel="Create"
            formFields={[
                { name: "nodes", label: "Input Nodes", type: "array", required: true },
                {
                    name: "password",
                    label: "Input Password",
                    type: "password",
                },
            ]}
            onSubmit={handleSubmit}
        >
            {children}
        </FormDialog>
    );
};

export const ImportCluster: React.FC<ClusterFormProps> = ({ position, namespace, children }) => {
    const router = useRouter();
    const handleSubmit = async (formData: FormData) => {
        const fieldsToValidate = ["nodes"];
        const errorMessage = validateFormData(formData, fieldsToValidate);
        if (errorMessage) {
            return errorMessage;
        }

        const formObj = Object.fromEntries(formData.entries());
        const nodes = JSON.parse(formObj["nodes"] as string) as string[];
        const cluster = formObj["cluster"] as string;
        const password = formObj["password"] as string;

        if (nodes.length === 0) {
            return "Nodes cannot be empty.";
        }

        for (const node of nodes) {
            if (containsWhitespace(node)) {
                return "Nodes cannot contain any whitespace characters.";
            }
        }

        const response = await importCluster(namespace, cluster, nodes, password);
        if (response === "") {
            router.push(`/namespaces/${namespace}/clusters/${cluster}`);
        } else {
            return response || "Failed to import cluster";
        }
    };

    return (
        <FormDialog
            position={position}
            title="Import Cluster"
            submitButtonLabel="Import"
            formFields={[
                {
                    name: "cluster",
                    label: "Input Cluster",
                    type: "text",
                    required: true,
                },
                { name: "nodes", label: "Input Nodes", type: "array", required: true },
                {
                    name: "password",
                    label: "Input Password",
                    type: "password",
                },
            ]}
            onSubmit={handleSubmit}
        >
            {children}
        </FormDialog>
    );
};

export const MigrateSlot: React.FC<ShardFormProps> = ({ position, namespace, cluster }) => {
    const router = useRouter();
    const handleSubmit = async (formData: FormData) => {
        const fieldsToValidate = ["target", "slot", "slot_only"];
        const errorMessage = validateFormData(formData, fieldsToValidate);
        if (errorMessage) {
            return errorMessage;
        }

        const formObj = Object.fromEntries(formData.entries());
        const target = parseInt(formObj["target"] as string);
        const slot = parseInt(formObj["slot"] as string);
        const slotOnly = formObj["slot_only"] === "true";

        // Basic Validation for numeric inputs
        if (isNaN(target) || target < 0) {
            return "Target shard index must be a valid non-negative number.";
        }
        if (isNaN(slot) || slot < 0 || slot > 16383) {
            return "Slot must be a valid number between 0 and 16383.";
        }

        try {
            const shards = await listShards(namespace, cluster);
            if (!shards || !Array.isArray(shards)) {
                return "Failed to fetch shard information. Please check if the cluster exists.";
            }

            if (target >= shards.length) {
                return `Target shard index ${target} does not exist. Available shards: 0-${shards.length - 1}.`;
            }

            let sourceShardIndex = -1;
            for (let i = 0; i < shards.length; i++) {
                const shard = shards[i] as any;
                if (shard.slot_ranges && Array.isArray(shard.slot_ranges)) {
                    for (const range of shard.slot_ranges) {
                        let start: number, end: number;
                        if (range.includes("-")) {
                            [start, end] = range.split("-").map(Number);
                        } else {
                            // Single slot, not a range
                            start = end = Number(range);
                        }
                        if (slot >= start && slot <= end) {
                            sourceShardIndex = i;
                            break;
                        }
                    }
                    if (sourceShardIndex !== -1) break;
                }
            }

            if (sourceShardIndex === target) {
                return `Cannot migrate slot ${slot} to the same shard (${target}). The slot is already in shard ${sourceShardIndex}.`;
            }

            if (sourceShardIndex === -1) {
                return `Slot ${slot} is not currently assigned to any shard and cannot be migrated.`;
            }

            const sourceShard = shards[sourceShardIndex] as any;
            if (sourceShard.migrating_slot === slot) {
                return `Slot ${slot} is already being migrated from shard ${sourceShardIndex}.`;
            }

            const targetShard = shards[target] as any;
            if (targetShard.import_slot === slot) {
                return `Slot ${slot} is already being imported to shard ${target}.`;
            }
        } catch (error) {
            console.error("Error validating migration:", error);
        }

        const response = await migrateSlot(namespace, cluster, target, slot, slotOnly);
        if (response === "") {
            window.location.reload();
        } else {
            // Handle specific error messages from the API
            if (response.includes("source and target shard is same")) {
                return "Migration failed: The source and target shards are the same. Please select a different target shard.";
            } else if (response.includes("the entry does not exist")) {
                return "Migration failed: The specified cluster, shard, or slot does not exist.";
            } else if (response.includes("already existed")) {
                return "Migration failed: The slot is already being migrated or exists in the target shard.";
            } else {
                return `Migration failed: ${response}`;
            }
        }
    };

    return (
        <FormDialog
            position={position}
            title="Migrate Slot"
            submitButtonLabel="Migrate"
            formFields={[
                { name: "target", label: "Target Shard Index", type: "text", required: true },
                { name: "slot", label: "Slot Number (0-16383)", type: "text", required: true },
                {
                    name: "slot_only",
                    label: "Slot Only Migration",
                    type: "enum",
                    values: ["false", "true"],
                    required: true,
                },
            ]}
            onSubmit={handleSubmit}
        />
    );
};

export const NodeCreation: React.FC<NodeFormProps> = ({
    position,
    namespace,
    cluster,
    shard,
    children,
}) => {
    const router = useRouter();

    const handleSubmit = async (formData: FormData) => {
        const fieldsToValidate = ["Address", "Role"];
        const errorMessage = validateFormData(formData, fieldsToValidate);
        if (errorMessage) {
            return errorMessage;
        }

        const formObj = Object.fromEntries(formData.entries());
        const address = formObj["Address"] as string;
        const role = formObj["Role"] as string;
        const password = formObj["Password"] as string;

        if (containsWhitespace(address)) {
            return "Address cannot contain any whitespace characters.";
        }

        const response = await createNode(namespace, cluster, shard, address, role, password);
        if (response === "") {
            window.location.reload();
        } else {
            return response || "Failed to create node";
        }
    };

    return (
        <FormDialog
            position={position}
            title="Create Node"
            submitButtonLabel="Create"
            formFields={[
                {
                    name: "Address",
                    label: "Input Address",
                    type: "text",
                    required: true,
                },
                {
                    name: "Role",
                    label: "Select Role",
                    type: "enum",
                    required: true,
                    values: ["master", "slave"],
                },
                {
                    name: "Password",
                    label: "Input Password",
                    type: "password",
                },
            ]}
            onSubmit={handleSubmit}
        >
            {children}
        </FormDialog>
    );
};
