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
    ListItem,
    ListItemButton,
    ListItemIcon,
    ListItemText,
    Menu,
    MenuItem,
    Snackbar,
    Tooltip,
    Badge,
} from "@mui/material";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import { useCallback, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import { useRouter } from "next/navigation";
import { deleteCluster, deleteNamespace, deleteNode, deleteShard } from "../lib/api";
import { faTrashCan } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import FolderIcon from "@mui/icons-material/Folder";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";

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

export default function Item(props: ItemProps) {
    const { item, type } = props;
    const [hover, setHover] = useState<boolean>(false);
    const [showMenu, setShowMenu] = useState<boolean>(false);
    const listItemRef = useRef(null);
    const openMenu = useCallback(() => setShowMenu(true), []);
    const closeMenu = useCallback(() => (setShowMenu(false), setHover(false)), []);
    const [showDeleteConfirm, setShowDeleteConfirm] = useState<boolean>(false);
    const openDeleteConfirmDialog = useCallback(
        () => (setShowDeleteConfirm(true), closeMenu()),
        [closeMenu]
    );
    const closeDeleteConfirmDialog = useCallback(() => setShowDeleteConfirm(false), []);
    const [errorMessage, setErrorMessage] = useState<string>("");

    const router = useRouter();
    let activeItem = usePathname().split("/").pop() || "";

    const getItemIcon = () => {
        switch (type) {
            case "namespace":
                return (
                    <FolderIcon fontSize="small" className="text-primary dark:text-primary-light" />
                );
            case "cluster":
                return (
                    <StorageIcon
                        fontSize="small"
                        className="text-primary dark:text-primary-light"
                    />
                );
            case "shard":
                return (
                    <DnsIcon fontSize="small" className="text-primary dark:text-primary-light" />
                );
            case "node":
                return (
                    <DeviceHubIcon
                        fontSize="small"
                        className="text-primary dark:text-primary-light"
                    />
                );
            default:
                return null;
        }
    };

    const confirmDelete = useCallback(async () => {
        let response = "";
        if (type === "namespace") {
            response = await deleteNamespace(item);
            if (response === "") {
                router.push("/namespaces");
            }
            setErrorMessage(response);
            router.refresh();
        } else if (type === "cluster") {
            const { namespace } = props as ClusterItemProps;
            response = await deleteCluster(namespace, item);
            if (response === "") {
                router.push(`/namespaces/${namespace}`);
            }
            setErrorMessage(response);
            router.refresh();
        } else if (type === "shard") {
            const { namespace, cluster } = props as ShardItemProps;
            response = await deleteShard(
                namespace,
                cluster,
                (parseInt(item.split("\t")[1]) - 1).toString()
            );
            if (response === "") {
                router.push(`/namespaces/${namespace}/clusters/${cluster}`);
            }
            setErrorMessage(response);
            router.refresh();
        } else if (type === "node") {
            const { namespace, cluster, shard, id } = props as NodeItemProps;
            response = await deleteNode(namespace, cluster, shard, id);
            if (response === "") {
                router.push(`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}`);
            }
            setErrorMessage(response);
            router.refresh();
        }
        closeMenu();
    }, [item, type, props, closeMenu, router]);

    if (type === "shard") {
        activeItem = "Shard\t" + (parseInt(activeItem) + 1);
    } else if (type === "node") {
        activeItem = "Node\t" + (parseInt(activeItem) + 1);
    }
    const isActive = item === activeItem;

    const displayName = item.includes("\t")
        ? item.split("\t")[0] + " " + item.split("\t")[1]
        : item;

    return (
        <ListItem
            disablePadding
            className="mb-1"
            ref={listItemRef}
            onMouseEnter={() => setHover(true)}
            onMouseLeave={() => !showMenu && setHover(false)}
        >
            <ListItemButton
                className={`group rounded-lg transition-all duration-200 ${
                    isActive
                        ? "bg-primary/10 text-primary shadow-sm dark:bg-primary-dark/20 dark:text-primary-light"
                        : "hover:bg-gray-100/80 hover:shadow-sm dark:hover:bg-dark-border/60"
                }`}
                dense
                sx={{
                    padding: "6px 10px",
                    borderRadius: "8px",
                }}
            >
                <ListItemIcon sx={{ minWidth: 32 }}>
                    <div
                        className={`flex h-6 w-6 items-center justify-center rounded-full ${isActive ? "bg-white/80 dark:bg-dark-paper/60" : "bg-gray-100 dark:bg-dark-border"}`}
                    >
                        {getItemIcon()}
                    </div>
                </ListItemIcon>
                <ListItemText
                    primary={displayName}
                    className="overflow-hidden text-ellipsis"
                    primaryTypographyProps={{
                        className: `text-sm font-medium ${isActive ? "text-primary dark:text-primary-light" : "text-gray-700 dark:text-gray-300"}`,
                        noWrap: true,
                    }}
                />
                {hover && (
                    <IconButton
                        size="small"
                        edge="end"
                        onClick={openMenu}
                        className="ml-1 opacity-0 transition-all duration-200 group-hover:opacity-100"
                        sx={{
                            width: 26,
                            height: 26,
                            backgroundColor: "rgba(0, 0, 0, 0.04)",
                            "&:hover": {
                                backgroundColor: "rgba(0, 0, 0, 0.08)",
                            },
                            ".dark &": {
                                backgroundColor: "rgba(255, 255, 255, 0.1)",
                            },
                            ".dark &:hover": {
                                backgroundColor: "rgba(255, 255, 255, 0.15)",
                            },
                        }}
                    >
                        <MoreVertIcon sx={{ fontSize: 16 }} />
                    </IconButton>
                )}
            </ListItemButton>

            <Menu
                id={`menu-${item}`}
                open={showMenu}
                onClose={closeMenu}
                anchorEl={listItemRef.current}
                anchorOrigin={{
                    vertical: "bottom",
                    horizontal: "right",
                }}
                transformOrigin={{
                    vertical: "top",
                    horizontal: "right",
                }}
                PaperProps={{
                    className: "shadow-lg rounded-lg",
                    elevation: 3,
                }}
            >
                <MenuItem
                    onClick={openDeleteConfirmDialog}
                    className="text-error hover:bg-error-light/10"
                >
                    <FontAwesomeIcon icon={faTrashCan} className="mr-2" />
                    Delete
                </MenuItem>
            </Menu>

            <Dialog
                open={showDeleteConfirm}
                onClose={closeDeleteConfirmDialog}
                className="backdrop-blur-sm"
                PaperProps={{
                    className: "rounded-xl shadow-xl",
                    sx: { minWidth: 320 },
                }}
            >
                <DialogTitle className="border-b border-gray-100 pb-3 font-semibold dark:border-gray-800">
                    Confirm Delete
                </DialogTitle>
                <DialogContent className="mt-4">
                    {type === "node" || type === "shard" ? (
                        <DialogContentText>
                            Are you sure you want to delete {displayName}?
                        </DialogContentText>
                    ) : (
                        <DialogContentText>
                            Are you sure you want to delete {type}{" "}
                            <span className="font-semibold">{item}</span>?
                        </DialogContentText>
                    )}
                </DialogContent>
                <DialogActions className="border-t border-gray-100 p-4 dark:border-gray-800">
                    <Button
                        onClick={closeDeleteConfirmDialog}
                        variant="outlined"
                        className="rounded-lg px-4"
                    >
                        Cancel
                    </Button>
                    <Button
                        onClick={confirmDelete}
                        variant="contained"
                        color="error"
                        className="rounded-lg bg-error px-4 hover:bg-error-dark"
                    >
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
                    sx={{ width: "100%", borderRadius: "8px" }}
                >
                    {errorMessage}
                </Alert>
            </Snackbar>
        </ListItem>
    );
}
