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

'use client';
import { Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, IconButton, ListItem, ListItemButton, ListItemText, Menu, MenuItem, Tooltip } from "@mui/material";
import MoreHorizIcon from '@mui/icons-material/MoreHoriz';
import { useCallback, useRef, useState } from "react";
import { deleteNamespaceAction } from "@/app/lib/actions";

export default function NamespaceItem({ item }: {item: string}) {
    const [hover, setHover] = useState<boolean>(false);
    const [showMenu, setShowMenu] = useState<boolean>(false);
    const listItemTextRef = useRef(null);
    const openMenu = useCallback(() => setShowMenu(true), []);
    const closeMenu = useCallback(() => (setShowMenu(false), setHover(false)), []);
    const [showDeleteConfirm, setShowDeleteConfirm] = useState<boolean>(false);
    const openDeleteConfirmDialog = useCallback(() => (setShowDeleteConfirm(true), closeMenu()), [closeMenu]);
    const closeDeleteConfirmDialog = useCallback(() => setShowDeleteConfirm(false), []);
    const confirmDelete = useCallback(async () => {
        await deleteNamespaceAction(item);
        closeMenu();
    },[item, closeMenu])
    return (<ListItem
        disablePadding
        secondaryAction={
            hover && <IconButton onClick={openMenu} ref={listItemTextRef} >
                <MoreHorizIcon />
            </IconButton>
        }
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => !showMenu && setHover(false)}
    >
        <ListItemButton sx={{paddingRight: '10px'}}>
            <Tooltip title={item} arrow>
                <ListItemText classes={{primary: 'overflow-hidden text-ellipsis text-nowrap'}} primary={`${item}`} />
            </Tooltip>
        </ListItemButton>
        <Menu
            id={item}
            open={showMenu}
            onClose={closeMenu}
            anchorEl={listItemTextRef.current}
            anchorOrigin={{
                vertical: 'center',
                horizontal: 'center',
            }}
        >
            <MenuItem color="red" onClick={openDeleteConfirmDialog}>Delete</MenuItem>
        </Menu>
        <Dialog
            open={showDeleteConfirm}
        >
            <DialogTitle>Confirm</DialogTitle>
            <DialogContent>
                <DialogContentText>
                    Please confirm you want to delete namespace {item}
                </DialogContentText>
            </DialogContent>
            <DialogActions>
                <Button onClick={closeDeleteConfirmDialog}>Cancel</Button>
                <Button onClick={confirmDelete} color="error">Delete</Button>
            </DialogActions>
        </Dialog>
    </ListItem>)
}
