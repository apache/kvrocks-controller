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

import {
    Alert,
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogContentText,
    DialogTitle,
    IconButton,
    Menu,
    MenuItem,
    Snackbar,
} from "@mui/material";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import FolderIcon from "@mui/icons-material/Folder";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import { useCallback, useRef, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { deleteCluster, deleteNamespace, deleteNode, deleteShard } from "../lib/api";

interface NamespaceItemProps {
    item: string;
    type: "namespace";
}

interface ClusterItemProps {
    item: string;
    type: "cluster";
    namespace: string;
}

interface ShardItemProps {
    item: string;
    type: "shard";
    namespace: string;
    cluster: string;
}

interface NodeItemProps {
    item: string;
    type: "node";
    namespace: string;
    cluster: string;
    shard: string;
    id: string;
}

type ItemProps = NamespaceItemProps | ClusterItemProps | ShardItemProps | NodeItemProps;

const iconFor = (type: ItemProps["type"]) => {
    const cls = "shrink-0 opacity-70";
    switch (type) {
        case "namespace":
            return <FolderIcon sx={{ fontSize: 16 }} className={cls} />;
        case "cluster":
            return <StorageIcon sx={{ fontSize: 16 }} className={cls} />;
        case "shard":
            return <DnsIcon sx={{ fontSize: 16 }} className={cls} />;
        case "node":
            return <DeviceHubIcon sx={{ fontSize: 16 }} className={cls} />;
    }
};

export default function Item(props: ItemProps) {
    const { item, type } = props;
    const [showMenu, setShowMenu] = useState(false);
    const [showConfirm, setShowConfirm] = useState(false);
    const [errorMessage, setErrorMessage] = useState("");
    const anchorRef = useRef<HTMLDivElement | null>(null);

    const router = useRouter();
    let activeSegment = usePathname().split("/").pop() || "";

    const openMenu = useCallback((e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setShowMenu(true);
    }, []);
    const closeMenu = useCallback(() => setShowMenu(false), []);

    const openConfirm = useCallback(() => {
        setShowConfirm(true);
        closeMenu();
    }, [closeMenu]);
    const closeConfirm = useCallback(() => setShowConfirm(false), []);

    const confirmDelete = useCallback(async () => {
        let response = "";
        if (type === "namespace") {
            response = await deleteNamespace(item);
            if (response === "") router.push("/namespaces");
        } else if (type === "cluster") {
            const { namespace } = props as ClusterItemProps;
            response = await deleteCluster(namespace, item);
            if (response === "") router.push(`/namespaces/${namespace}`);
        } else if (type === "shard") {
            const { namespace, cluster } = props as ShardItemProps;
            response = await deleteShard(
                namespace,
                cluster,
                (parseInt(item.split("\t")[1]) - 1).toString()
            );
            if (response === "") router.push(`/namespaces/${namespace}/clusters/${cluster}`);
        } else if (type === "node") {
            const { namespace, cluster, shard, id } = props as NodeItemProps;
            response = await deleteNode(namespace, cluster, shard, id);
            if (response === "")
                router.push(`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}`);
        }
        if (response) setErrorMessage(response);
        router.refresh();
        closeConfirm();
    }, [item, type, props, router, closeConfirm]);

    if (type === "shard") {
        activeSegment = "Shard\t" + (parseInt(activeSegment) + 1);
    } else if (type === "node") {
        activeSegment = "Node\t" + (parseInt(activeSegment) + 1);
    }
    const isActive = item === activeSegment;
    const displayName = item.includes("\t")
        ? item.split("\t")[0] + " " + item.split("\t")[1]
        : item;

    return (
        <>
            <div
                ref={anchorRef}
                className={`group flex h-9 cursor-pointer items-center gap-2.5 rounded-lg px-3 text-sm transition-colors ${
                    isActive
                        ? "bg-surface-hover font-medium text-text-primary dark:bg-surface-dark-hover dark:text-text-dark-primary"
                        : "text-text-secondary hover:bg-surface-hover hover:text-text-primary dark:text-text-dark-secondary dark:hover:bg-surface-dark-hover dark:hover:text-text-dark-primary"
                }`}
            >
                {iconFor(type)}
                <span className="flex-1 truncate">{displayName}</span>
                <IconButton
                    size="small"
                    onClick={openMenu}
                    className="opacity-0 transition-opacity group-hover:opacity-100"
                    sx={{
                        width: 24,
                        height: 24,
                        borderRadius: 1,
                        "& svg": { fontSize: 16 },
                    }}
                    aria-label="Item actions"
                >
                    <MoreHorizIcon />
                </IconButton>
            </div>

            <Menu
                open={showMenu}
                onClose={closeMenu}
                anchorEl={anchorRef.current}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                transformOrigin={{ vertical: "top", horizontal: "right" }}
            >
                <MenuItem onClick={openConfirm} sx={{ color: "error.main" }}>
                    <DeleteOutlineIcon sx={{ fontSize: 14, mr: 1 }} />
                    Delete
                </MenuItem>
            </Menu>

            <Dialog open={showConfirm} onClose={closeConfirm} maxWidth="xs" fullWidth>
                <DialogTitle>Delete {type}</DialogTitle>
                <DialogContent>
                    <DialogContentText sx={{ fontSize: "0.8125rem" }}>
                        {type === "node" || type === "shard" ? (
                            <>Delete {displayName}? This cannot be undone.</>
                        ) : (
                            <>
                                Delete {type} <b>{item}</b>? This cannot be undone.
                            </>
                        )}
                    </DialogContentText>
                </DialogContent>
                <DialogActions>
                    <Button onClick={closeConfirm} variant="outlined" size="small">
                        Cancel
                    </Button>
                    <Button onClick={confirmDelete} variant="contained" color="error" size="small">
                        Delete
                    </Button>
                </DialogActions>
            </Dialog>

            <Snackbar
                open={!!errorMessage}
                autoHideDuration={5000}
                onClose={() => setErrorMessage("")}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            >
                <Alert
                    onClose={() => setErrorMessage("")}
                    severity="error"
                    variant="filled"
                    sx={{ width: "100%" }}
                >
                    {errorMessage}
                </Alert>
            </Snackbar>
        </>
    );
}
