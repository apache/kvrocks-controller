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
    alpha,
    useTheme,
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
    const theme = useTheme();

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
                    className="rounded-full px-4 py-1.5 text-xs font-medium shadow-sm transition-all duration-200 hover:shadow-md"
                    startIcon={<AddIcon sx={{ fontSize: 16 }} />}
                    size="small"
                    sx={{
                        textTransform: "none",
                        "&:hover": {
                            transform: "translateY(-1px)",
                        },
                    }}
                >
                    {title}
                </Button>
            ) : (
                <Button
                    variant="outlined"
                    onClick={openDialog}
                    className="w-full rounded-xl border-2 py-2.5 font-medium shadow-sm transition-all duration-200 hover:bg-primary/5 hover:shadow-md dark:border-primary-dark/60 dark:hover:bg-primary-dark/10"
                    startIcon={<AddIcon />}
                    sx={{
                        textTransform: "none",
                        borderWidth: "1.5px",
                        "&:hover": {
                            borderWidth: "1.5px",
                        },
                    }}
                >
                    {title}
                </Button>
            )}

            <Dialog
                open={showDialog}
                onClose={closeDialog}
                PaperProps={{
                    className: "overflow-hidden",
                    sx: {
                        borderRadius: "16px",
                        boxShadow: "0 25px 50px -12px rgba(0, 0, 0, 0.25)",
                        maxWidth: "500px",
                        width: "100%",
                        backgroundImage:
                            theme.palette.mode === "dark"
                                ? "linear-gradient(to bottom, rgba(66, 66, 66, 0.8), rgba(33, 33, 33, 0.9))"
                                : "linear-gradient(to bottom, #ffffff, #f9fafb)",
                        backdropFilter: "blur(20px)",
                        overflow: "hidden",
                    },
                }}
                TransitionProps={{}}
            >
                <form onSubmit={handleSubmit}>
                    <DialogTitle
                        className="border-b border-light-border px-6 py-5 dark:border-dark-border"
                        sx={{
                            background:
                                theme.palette.mode === "dark"
                                    ? alpha(theme.palette.background.paper, 0.5)
                                    : alpha(theme.palette.primary.light, 0.05),
                        }}
                    >
                        <Typography
                            variant="h6"
                            className="font-semibold text-gray-800 dark:text-gray-100"
                        >
                            {title}
                        </Typography>
                    </DialogTitle>

                    <DialogContent
                        className="p-6"
                        sx={{
                            "&:first-of-type": {
                                paddingTop: "24px",
                            },
                        }}
                    >
                        {formFields.map((field, index) =>
                            field.type === "array" ? (
                                <Box key={index} mb={3} mt={index === 0 ? 0 : 2}>
                                    <Typography
                                        variant="subtitle2"
                                        className="mb-2 font-medium text-gray-700 dark:text-gray-300"
                                    >
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
                                                    className="rounded-full bg-primary-light/20 dark:bg-primary-dark/20"
                                                    sx={{
                                                        fontWeight: 500,
                                                        "& .MuiChip-deleteIcon": {
                                                            color:
                                                                theme.palette.mode === "dark"
                                                                    ? alpha(
                                                                          theme.palette.primary
                                                                              .light,
                                                                          0.7
                                                                      )
                                                                    : alpha(
                                                                          theme.palette.primary
                                                                              .main,
                                                                          0.7
                                                                      ),
                                                        },
                                                    }}
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
                                                className="rounded-xl bg-white dark:bg-dark-paper/70"
                                                sx={{
                                                    "& .MuiOutlinedInput-root": {
                                                        borderRadius: "12px",
                                                        transition: "all 0.2s ease",
                                                        "&.Mui-focused": {
                                                            boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
                                                        },
                                                    },
                                                }}
                                            />
                                        )}
                                    />
                                </Box>
                            ) : field.type === "enum" ? (
                                <FormControl
                                    key={index}
                                    fullWidth
                                    sx={{ mt: index === 0 ? 0 : 3, mb: 2 }}
                                >
                                    <InputLabel id={`${field.name}-label`} className="font-medium">
                                        {field.label}
                                    </InputLabel>
                                    <Select
                                        labelId={`${field.name}-label`}
                                        name={field.name}
                                        label={field.label}
                                        required={field.required}
                                        defaultValue=""
                                        size="small"
                                        className="rounded-xl bg-white dark:bg-dark-paper/70"
                                        sx={{
                                            borderRadius: "12px",
                                            "& .MuiOutlinedInput-notchedOutline": {
                                                borderColor: alpha(theme.palette.divider, 0.8),
                                            },
                                            "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                                                boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
                                            },
                                            "& .MuiSelect-select": {
                                                display: "flex",
                                                alignItems: "center",
                                                minHeight: "32px",
                                            },
                                        }}
                                        MenuProps={{
                                            PaperProps: {
                                                sx: {
                                                    borderRadius: "12px",
                                                    boxShadow:
                                                        "0 10px 25px -5px rgba(0, 0, 0, 0.1)",
                                                    mt: 1,
                                                },
                                            },
                                        }}
                                    >
                                        {field.values?.map((value, index) => (
                                            <MenuItem
                                                key={index}
                                                value={value}
                                                sx={{
                                                    minHeight: "32px",
                                                    display: "flex",
                                                    alignItems: "center",
                                                }}
                                            >
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
                                    className="bg-white dark:bg-dark-paper/70"
                                    sx={{
                                        mt: index === 0 ? 0 : 3,
                                        mb: 1.5,
                                        "& .MuiOutlinedInput-root": {
                                            borderRadius: "12px",
                                            transition: "all 0.2s ease",
                                            "&.Mui-focused": {
                                                boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
                                            },
                                        },
                                        "& .MuiInputLabel-root": {
                                            fontWeight: 500,
                                        },
                                    }}
                                />
                            )
                        )}
                    </DialogContent>

                    <DialogActions
                        className="border-t border-light-border p-4 dark:border-dark-border"
                        sx={{
                            background:
                                theme.palette.mode === "dark"
                                    ? alpha(theme.palette.background.paper, 0.5)
                                    : alpha(theme.palette.primary.light, 0.05),
                            padding: "16px 24px",
                            justifyContent: "space-between",
                        }}
                    >
                        <Button
                            onClick={closeDialog}
                            disabled={submitting}
                            className="rounded-xl px-4 py-1.5 text-gray-600 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-dark-border"
                            sx={{
                                textTransform: "none",
                                fontWeight: 500,
                                transition: "all 0.2s ease",
                            }}
                        >
                            Cancel
                        </Button>
                        <Button
                            type="submit"
                            variant="contained"
                            disabled={submitting}
                            className="rounded-xl px-5 py-1.5 shadow-sm transition-all duration-200 hover:shadow-md"
                            sx={{
                                textTransform: "none",
                                fontWeight: 600,
                                transition: "all 0.2s ease",
                                "&:hover": {
                                    transform: "translateY(-1px)",
                                    boxShadow: "0 6px 15px rgba(0, 0, 0, 0.1)",
                                },
                            }}
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
                    className="rounded-xl shadow-lg"
                    sx={{
                        borderRadius: "12px",
                        boxShadow: "0 10px 30px rgba(0, 0, 0, 0.15)",
                    }}
                >
                    {errorMessage}
                </Alert>
            </Snackbar>
        </>
    );
};

export default FormDialog;
