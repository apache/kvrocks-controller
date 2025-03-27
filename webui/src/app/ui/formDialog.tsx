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

import {
    Alert,
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogContentText,
    DialogTitle,
    Snackbar,
    TextField,
    Box,
    Chip,
    Typography,
    Autocomplete,
    MenuItem,
    Select,
    InputLabel,
    FormControl,
    Paper,
    CircularProgress,
} from "@mui/material";
import React, { useCallback, useState, FormEvent } from "react";
import AddIcon from "@mui/icons-material/Add";

interface FormDialogProps {
    position: string;
    title: string;
    submitButtonLabel: string;
    formFields: {
        name: string;
        label: string;
        type: string;
        required?: boolean;
        values?: string[];
    }[];
    onSubmit: (formData: FormData) => Promise<string | undefined>;
}

const FormDialog: React.FC<FormDialogProps> = ({
    position,
    title,
    submitButtonLabel,
    formFields,
    onSubmit,
}) => {
    const [showDialog, setShowDialog] = useState(false);
    const openDialog = useCallback(() => setShowDialog(true), []);
    const closeDialog = useCallback(() => setShowDialog(false), []);
    const [errorMessage, setErrorMessage] = useState("");
    const [arrayValues, setArrayValues] = useState<{ [key: string]: string[] }>({});
    const [submitting, setSubmitting] = useState(false);

    const handleArrayChange = (name: string, value: string[]) => {
        setArrayValues({ ...arrayValues, [name]: value });
    };

    const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        setSubmitting(true);
        const formData = new FormData(event.currentTarget);

        Object.keys(arrayValues).forEach((name) => {
            formData.append(name, JSON.stringify(arrayValues[name]));
        });

        try {
            const error = await onSubmit(formData);
            if (error) {
                setErrorMessage(error);
            } else {
                closeDialog();
            }
        } catch (error) {
            setErrorMessage("An unexpected error occurred");
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <>
            {position === "card" ? (
                <Button
                    variant="contained"
                    onClick={openDialog}
                    className="btn btn-primary px-3 py-1 text-xs"
                    startIcon={<AddIcon sx={{ fontSize: 16 }} />}
                    size="small"
                >
                    {title}
                </Button>
            ) : (
                <Button
                    variant="outlined"
                    onClick={openDialog}
                    className="btn btn-outline w-full"
                    startIcon={<AddIcon />}
                >
                    {title}
                </Button>
            )}

            <Dialog
                open={showDialog}
                onClose={closeDialog}
                PaperProps={{
                    className: "rounded-lg shadow-xl",
                }}
                maxWidth="sm"
                fullWidth
            >
                <form onSubmit={handleSubmit}>
                    <DialogTitle className="border-b border-light-border bg-gray-50 px-6 py-4 dark:border-dark-border dark:bg-dark-paper">
                        <Typography variant="h6" className="font-medium">
                            {title}
                        </Typography>
                    </DialogTitle>
                    <DialogContent className="p-6">
                        {formFields.map((field, index) =>
                            field.type === "array" ? (
                                <Box key={index} mb={3} mt={index === 0 ? 3 : 2}>
                                    <Typography variant="subtitle2" className="mb-2 font-medium">
                                        {field.label}
                                    </Typography>
                                    <Autocomplete
                                        multiple
                                        freeSolo
                                        value={arrayValues[field.name] || []}
                                        options={[]}
                                        onChange={(event, newValue) =>
                                            handleArrayChange(field.name, newValue)
                                        }
                                        renderTags={(value, getTagProps) =>
                                            value.map((option, index) => (
                                                <Chip
                                                    {...getTagProps({ index })}
                                                    key={index}
                                                    label={option}
                                                    size="small"
                                                    className="bg-primary-light/20 dark:bg-primary-dark/20"
                                                />
                                            ))
                                        }
                                        renderInput={(params) => (
                                            <TextField
                                                {...params}
                                                variant="outlined"
                                                label={`Add ${field.label}*`}
                                                placeholder="Type and press enter"
                                                size="small"
                                                className="rounded-md bg-white dark:bg-dark-paper"
                                            />
                                        )}
                                    />
                                </Box>
                            ) : field.type === "enum" ? (
                                <FormControl
                                    key={index}
                                    fullWidth
                                    sx={{ mt: index === 0 ? 3 : 3, mb: 2 }}
                                >
                                    <InputLabel id={`${field.name}-label`}>
                                        {field.label}
                                    </InputLabel>
                                    <Select
                                        labelId={`${field.name}-label`}
                                        name={field.name}
                                        label={field.label}
                                        required={field.required}
                                        defaultValue=""
                                        size="small"
                                        className="rounded-md bg-white dark:bg-dark-paper"
                                    >
                                        {field.values?.map((value, index) => (
                                            <MenuItem key={index} value={value}>
                                                {value}
                                            </MenuItem>
                                        ))}
                                    </Select>
                                </FormControl>
                            ) : (
                                <TextField
                                    key={index}
                                    autoFocus={index === 0}
                                    required={field.required}
                                    name={field.name}
                                    label={field.label}
                                    type={field.type}
                                    fullWidth
                                    variant="outlined"
                                    margin="normal"
                                    size="small"
                                    className="rounded-md bg-white dark:bg-dark-paper"
                                    sx={{
                                        mt: index === 0 ? 3 : 3,
                                        mb: 1.5,
                                    }}
                                />
                            )
                        )}
                    </DialogContent>
                    <DialogActions className="border-t border-light-border bg-gray-50 p-4 dark:border-dark-border dark:bg-dark-paper">
                        <Button
                            onClick={closeDialog}
                            disabled={submitting}
                            className="text-gray-600 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-dark-border"
                        >
                            Cancel
                        </Button>
                        <Button
                            type="submit"
                            variant="contained"
                            disabled={submitting}
                            className="btn-primary"
                            startIcon={
                                submitting ? <CircularProgress size={16} color="inherit" /> : null
                            }
                        >
                            {submitting ? "Processing..." : submitButtonLabel}
                        </Button>
                    </DialogActions>
                </form>
            </Dialog>
            <Snackbar
                open={!!errorMessage}
                autoHideDuration={5000}
                onClose={() => setErrorMessage("")}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                className="mb-4"
            >
                <Alert
                    onClose={() => setErrorMessage("")}
                    severity="error"
                    variant="filled"
                    className="shadow-lg"
                >
                    {errorMessage}
                </Alert>
            </Snackbar>
        </>
    );
};

export default FormDialog;
