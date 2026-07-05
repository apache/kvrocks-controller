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
    Autocomplete,
    Button,
    Chip,
    CircularProgress,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    FormControl,
    InputLabel,
    MenuItem,
    Select,
    Snackbar,
    TextField,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import React, { FormEvent, useCallback, useState } from "react";

interface FormDialogProps {
    position: string;
    title: string;
    submitButtonLabel: string;
    triggerLabel?: string;
    triggerIcon?: React.ReactNode;
    emphasis?: "primary" | "secondary";
    formFields: {
        name: string;
        label: string;
        type: string;
        required?: boolean;
        values?: string[];
    }[];
    onSubmit: (formData: FormData) => Promise<string | undefined>;
    children?: React.ReactNode;
}

const FormDialog: React.FC<FormDialogProps> = ({
    position,
    title,
    submitButtonLabel,
    triggerLabel,
    triggerIcon,
    emphasis,
    formFields,
    onSubmit,
    children,
}) => {
    const [open, setOpen] = useState(false);
    const [error, setError] = useState("");
    const [submitting, setSubmitting] = useState(false);
    const [arrayValues, setArrayValues] = useState<Record<string, string[]>>({});

    const openDialog = useCallback(() => setOpen(true), []);
    const closeDialog = useCallback(() => setOpen(false), []);

    const handleArrayChange = (name: string, value: string[]) => {
        setArrayValues((prev) => ({ ...prev, [name]: value }));
    };

    const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        setSubmitting(true);
        const formData = new FormData(event.currentTarget);
        Object.keys(arrayValues).forEach((name) => {
            formData.append(name, JSON.stringify(arrayValues[name]));
        });

        try {
            const err = await onSubmit(formData);
            if (err) setError(err);
            else closeDialog();
        } catch {
            setError("An unexpected error occurred");
        } finally {
            setSubmitting(false);
        }
    };

    const label = triggerLabel ?? title;
    const icon = triggerIcon ?? <AddIcon sx={{ fontSize: 13 }} />;

    let trigger: React.ReactNode;
    if (children) {
        trigger = <div onClick={openDialog}>{children}</div>;
    } else if (position === "card") {
        trigger = (
            <Button variant="contained" size="small" onClick={openDialog} startIcon={icon}>
                {label}
            </Button>
        );
    } else if (position === "page") {
        // Page-level triggers sit next to search and other actions; content-sized,
        // with primary/secondary distinction so callers can express hierarchy.
        trigger = (
            <Button
                variant={emphasis === "primary" ? "contained" : "outlined"}
                size="small"
                onClick={openDialog}
                startIcon={icon}
            >
                {label}
            </Button>
        );
    } else {
        // Sidebar (and any legacy caller) — full-width. `emphasis="primary"`
        // upgrades it to the brand-filled CTA used for the top-level create action;
        // everything else stays as an unobtrusive outlined button.
        const isPrimary = emphasis === "primary";
        trigger = (
            <Button
                variant={isPrimary ? "contained" : "outlined"}
                color="primary"
                size="small"
                fullWidth
                disableElevation
                onClick={openDialog}
                startIcon={icon}
                sx={{
                    height: 32,
                    borderRadius: "6px",
                    fontWeight: 500,
                    letterSpacing: "-0.006em",
                    ...(isPrimary
                        ? {
                              boxShadow: "none",
                              "&:hover": { boxShadow: "none" },
                          }
                        : {}),
                }}
            >
                {label}
            </Button>
        );
    }

    return (
        <>
            {trigger}

            <Dialog open={open} onClose={closeDialog} maxWidth="xs" fullWidth>
                <form onSubmit={handleSubmit}>
                    <DialogTitle>{title}</DialogTitle>

                    <DialogContent>
                        <div className="flex flex-col gap-3">
                            {formFields.map((field, index) => {
                                if (field.type === "array") {
                                    return (
                                        <div key={index}>
                                            <label className="mb-1 block text-xs font-medium text-text-secondary dark:text-text-dark-secondary">
                                                {field.label}
                                            </label>
                                            <Autocomplete
                                                multiple
                                                freeSolo
                                                size="small"
                                                value={arrayValues[field.name] || []}
                                                options={[]}
                                                onChange={(_, value) =>
                                                    handleArrayChange(field.name, value)
                                                }
                                                renderTags={(value, getTagProps) =>
                                                    value.map((option, i) => (
                                                        <Chip
                                                            {...getTagProps({ index: i })}
                                                            key={i}
                                                            label={option}
                                                            size="small"
                                                        />
                                                    ))
                                                }
                                                renderInput={(params) => (
                                                    <TextField
                                                        {...params}
                                                        placeholder="Type and press Enter"
                                                    />
                                                )}
                                            />
                                        </div>
                                    );
                                }
                                if (field.type === "enum") {
                                    return (
                                        <FormControl fullWidth size="small" key={index}>
                                            <InputLabel id={`${field.name}-label`}>
                                                {field.label}
                                            </InputLabel>
                                            <Select
                                                labelId={`${field.name}-label`}
                                                name={field.name}
                                                label={field.label}
                                                required={field.required}
                                                defaultValue=""
                                            >
                                                {field.values?.map((value) => (
                                                    <MenuItem key={value} value={value}>
                                                        {value}
                                                    </MenuItem>
                                                ))}
                                            </Select>
                                        </FormControl>
                                    );
                                }
                                return (
                                    <TextField
                                        key={index}
                                        autoFocus={index === 0}
                                        required={field.required}
                                        name={field.name}
                                        label={field.label}
                                        type={field.type}
                                        fullWidth
                                        size="small"
                                    />
                                );
                            })}
                        </div>
                    </DialogContent>

                    <DialogActions>
                        <Button
                            onClick={closeDialog}
                            variant="outlined"
                            size="small"
                            disabled={submitting}
                        >
                            Cancel
                        </Button>
                        <Button
                            type="submit"
                            variant="contained"
                            size="small"
                            disabled={submitting}
                            startIcon={
                                submitting ? <CircularProgress size={12} color="inherit" /> : null
                            }
                        >
                            {submitting ? "Working…" : submitButtonLabel}
                        </Button>
                    </DialogActions>
                </form>
            </Dialog>

            <Snackbar
                open={!!error}
                autoHideDuration={5000}
                onClose={() => setError("")}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            >
                <Alert onClose={() => setError("")} severity="error" variant="filled">
                    {error}
                </Alert>
            </Snackbar>
        </>
    );
};

export default FormDialog;
