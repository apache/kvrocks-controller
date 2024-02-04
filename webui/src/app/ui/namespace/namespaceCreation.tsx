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
import { createNamespaceAction } from "@/app/lib/actions";
import { Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, TextField } from "@mui/material";
import { useCallback, useState } from "react";

export default function NamespaceCreation() {
    const [showDialog, setShowDialog] = useState(false);
    const openDialog = useCallback(() => setShowDialog(true), []);
    const closeDialog = useCallback(() => setShowDialog(false), []);
    return (
        <>
            <Button variant="outlined" onClick={openDialog}>Create Namespace</Button>
            <Dialog
                open={showDialog}
                PaperProps={{
                    component: 'form',
                    action: async (formData: FormData) => {
                        const success = await createNamespaceAction(formData);
                        if(success) {
                            closeDialog();
                        } else {
                            //todo: error handle
                        }
                    },
                }}
                onClose={closeDialog}
            >
                <DialogTitle>Create Namespace</DialogTitle>
                <DialogContent
                    sx={{
                        width: '500px'
                    }}
                >
                    <TextField
                        autoFocus
                        required
                        name="name"
                        label="Input Name"
                        type="name"
                        fullWidth
                        variant="standard"
                    />
                </DialogContent>
                <DialogActions>
                    <Button onClick={closeDialog}>Cancel</Button>
                    <Button type="submit">Create</Button>
                </DialogActions>
            </Dialog>
        </>
    )
}
