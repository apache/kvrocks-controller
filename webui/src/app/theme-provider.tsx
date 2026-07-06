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

import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { ThemeProvider as MuiThemeProvider, createTheme } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";

type ThemeContextType = {
    isDarkMode: boolean;
    toggleTheme: () => void;
};

const ThemeContext = createContext<ThemeContextType>({
    isDarkMode: false,
    toggleTheme: () => {},
});

export const useTheme = () => useContext(ThemeContext);

// Linear-style tokens — must stay in sync with tailwind.config.ts and globals.css.
const tokens = {
    light: {
        background: "#ffffff",
        surface: "#fbfbfc",
        surfaceMuted: "#f4f5f8",
        border: "#e6e7eb",
        borderStrong: "#d0d3d9",
        textPrimary: "#0d0e10",
        textSecondary: "#4b4f57",
        textMuted: "#6b7280",
        accent: "#5e6ad2",
        accentHover: "#4f5abd",
    },
    dark: {
        background: "#08090a",
        surface: "#101113",
        surfaceMuted: "#181a1f",
        border: "#23252a",
        borderStrong: "#33363d",
        textPrimary: "#f7f8f8",
        textSecondary: "#b4b8c0",
        textMuted: "#8a8f98",
        accent: "#7079e0",
        accentHover: "#8d95f2",
    },
};

export function ThemeProvider({ children }: { children: React.ReactNode }) {
    const [isDarkMode, setIsDarkMode] = useState(false);

    useEffect(() => {
        const storedTheme = localStorage.getItem("theme");
        const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
        const shouldBeDark = storedTheme === "dark" || (!storedTheme && prefersDark);

        if (shouldBeDark) {
            document.documentElement.classList.add("dark");
        } else {
            document.documentElement.classList.remove("dark");
        }
        setIsDarkMode(shouldBeDark);
    }, []);

    const toggleTheme = () => {
        setIsDarkMode((prev) => {
            const next = !prev;
            if (next) {
                document.documentElement.classList.add("dark");
                localStorage.setItem("theme", "dark");
            } else {
                document.documentElement.classList.remove("dark");
                localStorage.setItem("theme", "light");
            }
            return next;
        });
    };

    const theme = useMemo(() => {
        const t = isDarkMode ? tokens.dark : tokens.light;

        return createTheme({
            palette: {
                mode: isDarkMode ? "dark" : "light",
                primary: {
                    main: t.accent,
                    light: isDarkMode ? "#8d95f2" : "#8d95f2",
                    dark: t.accentHover,
                    contrastText: "#ffffff",
                },
                secondary: {
                    main: "#7170ff",
                    light: "#a5a4ff",
                    dark: "#5352d9",
                    contrastText: "#ffffff",
                },
                error: {
                    main: "#eb5757",
                },
                warning: {
                    main: "#f2c94c",
                },
                success: {
                    main: "#4cb782",
                },
                info: {
                    main: "#26b5ce",
                },
                background: {
                    default: t.background,
                    paper: t.surface,
                },
                text: {
                    primary: t.textPrimary,
                    secondary: t.textSecondary,
                    disabled: t.textMuted,
                },
                divider: t.border,
            },
            shape: {
                borderRadius: 6,
            },
            typography: {
                fontFamily:
                    'InterVariable, Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif',
                fontSize: 15,
                htmlFontSize: 16,
                h1: { fontSize: "2.25rem", fontWeight: 600, letterSpacing: "-0.02em" },
                h2: { fontSize: "1.75rem", fontWeight: 600, letterSpacing: "-0.015em" },
                h3: { fontSize: "1.5rem", fontWeight: 600, letterSpacing: "-0.01em" },
                h4: { fontSize: "1.25rem", fontWeight: 600, letterSpacing: "-0.005em" },
                h5: { fontSize: "1.125rem", fontWeight: 600 },
                h6: { fontSize: "1rem", fontWeight: 600 },
                subtitle1: { fontSize: "0.9375rem", fontWeight: 500 },
                subtitle2: { fontSize: "0.875rem", fontWeight: 500 },
                body1: { fontSize: "0.9375rem", lineHeight: 1.55 },
                body2: { fontSize: "0.875rem", lineHeight: 1.55, color: t.textSecondary },
                button: { fontSize: "0.875rem", fontWeight: 500, textTransform: "none" },
                caption: { fontSize: "0.8125rem", color: t.textMuted },
                overline: {
                    fontSize: "0.6875rem",
                    fontWeight: 500,
                    letterSpacing: "0.06em",
                    textTransform: "uppercase",
                    color: t.textMuted,
                },
            },
            components: {
                MuiCssBaseline: {
                    styleOverrides: {
                        body: {
                            backgroundColor: t.background,
                            color: t.textPrimary,
                        },
                    },
                },
                MuiPaper: {
                    defaultProps: { elevation: 0 },
                    styleOverrides: {
                        root: {
                            backgroundImage: "none",
                            backgroundColor: t.background,
                            border: `1px solid ${t.border}`,
                            borderRadius: 8,
                        },
                    },
                },
                MuiAppBar: {
                    styleOverrides: {
                        root: {
                            boxShadow: "none",
                            backgroundColor: t.background,
                            color: t.textPrimary,
                            borderBottom: `1px solid ${t.border}`,
                        },
                    },
                },
                MuiButton: {
                    defaultProps: { disableElevation: true, disableRipple: false },
                    styleOverrides: {
                        root: {
                            textTransform: "none",
                            fontWeight: 500,
                            borderRadius: 8,
                            paddingInline: 14,
                            paddingBlock: 7,
                            minHeight: 36,
                            boxShadow: "none",
                            "&:hover": { boxShadow: "none" },
                        },
                        sizeSmall: { minHeight: 30, paddingInline: 12, fontSize: "0.8125rem" },
                        sizeLarge: { minHeight: 44, paddingInline: 20, fontSize: "1rem" },
                        contained: {
                            backgroundColor: t.accent,
                            color: "#ffffff",
                            "&:hover": { backgroundColor: t.accentHover },
                        },
                        outlined: {
                            borderColor: t.border,
                            color: t.textPrimary,
                            backgroundColor: "transparent",
                            "&:hover": {
                                borderColor: t.borderStrong,
                                backgroundColor: isDarkMode
                                    ? "rgba(255,255,255,0.04)"
                                    : "rgba(0,0,0,0.03)",
                            },
                        },
                        text: {
                            color: t.textSecondary,
                            "&:hover": {
                                backgroundColor: isDarkMode
                                    ? "rgba(255,255,255,0.05)"
                                    : "rgba(0,0,0,0.04)",
                            },
                        },
                    },
                },
                MuiIconButton: {
                    styleOverrides: {
                        root: {
                            borderRadius: 8,
                            color: t.textSecondary,
                            "&:hover": {
                                backgroundColor: isDarkMode
                                    ? "rgba(255,255,255,0.06)"
                                    : "rgba(0,0,0,0.05)",
                            },
                        },
                        sizeSmall: { width: 34, height: 34 },
                    },
                },
                MuiChip: {
                    styleOverrides: {
                        root: {
                            borderRadius: 6,
                            fontSize: "0.75rem",
                            fontWeight: 500,
                            height: 26,
                            border: `1px solid ${t.border}`,
                            backgroundColor: t.surfaceMuted,
                            color: t.textSecondary,
                            "& .MuiChip-icon": { color: "inherit", fontSize: 15, marginLeft: 6 },
                            "& .MuiChip-label": { paddingInline: 10 },
                        },
                        outlined: {
                            backgroundColor: "transparent",
                        },
                        sizeSmall: { height: 24, fontSize: "0.75rem" },
                    },
                },
                MuiDialog: {
                    styleOverrides: {
                        paper: {
                            backgroundImage: "none",
                            borderRadius: 10,
                            border: `1px solid ${t.border}`,
                            boxShadow: isDarkMode
                                ? "0 20px 60px -20px rgba(0,0,0,0.6), 0 0 0 1px rgba(255,255,255,0.06)"
                                : "0 20px 60px -20px rgba(15,17,22,0.2), 0 0 0 1px rgba(15,17,22,0.06)",
                        },
                    },
                },
                MuiDialogTitle: {
                    styleOverrides: {
                        root: {
                            fontSize: "1.0625rem",
                            fontWeight: 600,
                            padding: "18px 24px",
                            borderBottom: `1px solid ${t.border}`,
                        },
                    },
                },
                MuiDialogContent: {
                    styleOverrides: {
                        root: { padding: "24px", "&.MuiDialogContent-root": { paddingTop: 24 } },
                    },
                },
                MuiDialogActions: {
                    styleOverrides: {
                        root: {
                            padding: "16px 24px",
                            borderTop: `1px solid ${t.border}`,
                            gap: 10,
                        },
                    },
                },
                MuiOutlinedInput: {
                    styleOverrides: {
                        root: {
                            borderRadius: 8,
                            backgroundColor: t.background,
                            fontSize: "0.875rem",
                            "& .MuiOutlinedInput-notchedOutline": {
                                borderColor: t.border,
                            },
                            "&:hover .MuiOutlinedInput-notchedOutline": {
                                borderColor: t.borderStrong,
                            },
                            "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                                borderColor: t.accent,
                                borderWidth: 1,
                                boxShadow: "0 0 0 2px rgba(94, 106, 210, 0.2)",
                            },
                        },
                        input: { padding: "10px 12px" },
                    },
                },
                MuiInputLabel: {
                    styleOverrides: {
                        root: { fontSize: "0.875rem" },
                    },
                },
                MuiListItemButton: {
                    styleOverrides: {
                        root: {
                            borderRadius: 6,
                            "&.Mui-selected": {
                                backgroundColor: isDarkMode
                                    ? "rgba(112,121,224,0.16)"
                                    : "rgba(94,106,210,0.1)",
                                color: t.accent,
                                "&:hover": {
                                    backgroundColor: isDarkMode
                                        ? "rgba(112,121,224,0.22)"
                                        : "rgba(94,106,210,0.14)",
                                },
                            },
                        },
                    },
                },
                MuiMenu: {
                    styleOverrides: {
                        paper: {
                            border: `1px solid ${t.border}`,
                            borderRadius: 8,
                            marginTop: 4,
                            boxShadow: isDarkMode
                                ? "0 4px 16px -4px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.06)"
                                : "0 4px 16px -4px rgba(15,17,22,0.1), 0 0 0 1px rgba(15,17,22,0.06)",
                        },
                    },
                },
                MuiMenuItem: {
                    styleOverrides: {
                        root: {
                            fontSize: "0.875rem",
                            paddingBlock: 8,
                            paddingInline: 12,
                            borderRadius: 6,
                            marginInline: 4,
                        },
                    },
                },
                MuiPopover: {
                    styleOverrides: {
                        paper: {
                            border: `1px solid ${t.border}`,
                            borderRadius: 8,
                            boxShadow: isDarkMode
                                ? "0 4px 16px -4px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.06)"
                                : "0 4px 16px -4px rgba(15,17,22,0.1), 0 0 0 1px rgba(15,17,22,0.06)",
                        },
                    },
                },
                MuiTooltip: {
                    styleOverrides: {
                        tooltip: {
                            backgroundColor: isDarkMode ? "#22262f" : "#0d0e10",
                            color: isDarkMode ? "#f7f8f8" : "#ffffff",
                            fontSize: "0.6875rem",
                            fontWeight: 500,
                            padding: "5px 8px",
                            borderRadius: 4,
                        },
                        arrow: {
                            color: isDarkMode ? "#22262f" : "#0d0e10",
                        },
                    },
                },
                MuiDivider: {
                    styleOverrides: {
                        root: { borderColor: t.border },
                    },
                },
                MuiAlert: {
                    styleOverrides: {
                        root: { borderRadius: 8, fontSize: "0.875rem", padding: "10px 14px" },
                    },
                },
                MuiSwitch: {
                    styleOverrides: {
                        switchBase: {
                            "&.Mui-checked": { color: t.accent },
                            "&.Mui-checked + .MuiSwitch-track": {
                                backgroundColor: t.accent,
                                opacity: 0.6,
                            },
                        },
                    },
                },
            },
        });
    }, [isDarkMode]);

    return (
        <ThemeContext.Provider value={{ isDarkMode, toggleTheme }}>
            <MuiThemeProvider theme={theme}>
                <CssBaseline />
                {children}
            </MuiThemeProvider>
        </ThemeContext.Provider>
    );
}
