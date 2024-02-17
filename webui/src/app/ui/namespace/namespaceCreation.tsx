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
import { Alert, Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, Snackbar, TextField } from "@mui/material";
import { useCallback, useState } from "react";

export default function NamespaceCreation() {
    const [showDialog, setShowDialog] = useState(false);
    const openDialog = useCallback(() => setShowDialog(true), []);
    const closeDialog = useCallback(() => setShowDialog(false), []);
    const [errorMessage, setErrorMessage] = useState('');

    return (
        <>
            <Button variant="outlined" onClick={openDialog}>Create Namespace</Button>
            <Dialog
                open={showDialog}
                PaperProps={{
                    component: 'form',
                    action: async (formData: FormData) => {
                        const formObj = Object.fromEntries(formData.entries());
                        if(typeof formObj['name'] === 'string') {
                            const errMsg = await createNamespaceAction(formObj['name']);
                            if (errMsg) {
                                setErrorMessage(errMsg);
                            }
                            closeDialog();
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
            <Snackbar
                open={!!errorMessage}
                autoHideDuration={5000}
                onClose={() => setErrorMessage('')}
                anchorOrigin={{vertical: 'bottom', horizontal: 'right'}}
            >
                <Alert
                    onClose={() => setErrorMessage('')}
                    severity="error"
                    variant="filled"
                    sx={{ width: '100%' }}
                >
                    {errorMessage}
                </Alert>
            </Snackbar>
        </>
    )
}
