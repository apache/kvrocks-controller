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

import type { Config } from "tailwindcss";

const config: Config = {
    content: [
        "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
        "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
        "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
    ],
    darkMode: "class",
    theme: {
        extend: {
            fontFamily: {
                sans: [
                    "InterVariable",
                    "Inter",
                    "-apple-system",
                    "BlinkMacSystemFont",
                    "Segoe UI",
                    "Helvetica",
                    "Arial",
                    "sans-serif",
                ],
                mono: [
                    "ui-monospace",
                    "SFMono-Regular",
                    "SF Mono",
                    "Menlo",
                    "Consolas",
                    "Liberation Mono",
                    "monospace",
                ],
            },
            fontSize: {
                "2xs": ["0.6875rem", { lineHeight: "1rem" }],
                xs: ["0.8125rem", { lineHeight: "1.125rem" }],
                sm: ["0.875rem", { lineHeight: "1.25rem" }],
                base: ["0.9375rem", { lineHeight: "1.4rem" }],
                lg: ["1.0625rem", { lineHeight: "1.55rem" }],
                xl: ["1.25rem", { lineHeight: "1.65rem" }],
                "2xl": ["1.5rem", { lineHeight: "1.9rem" }],
                "3xl": ["1.875rem", { lineHeight: "2.25rem" }],
                "4xl": ["2.25rem", { lineHeight: "2.625rem" }],
                "5xl": ["3rem", { lineHeight: "3.25rem" }],
            },
            colors: {
                // Linear brand accent (indigo/violet)
                primary: {
                    DEFAULT: "#5e6ad2",
                    light: "#8d95f2",
                    dark: "#4a54b8",
                    contrastText: "#ffffff",
                },
                secondary: {
                    DEFAULT: "#7170ff",
                    light: "#a5a4ff",
                    dark: "#5352d9",
                    contrastText: "#ffffff",
                },
                success: {
                    DEFAULT: "#4cb782",
                    light: "#68d9a0",
                    dark: "#2f8f5f",
                },
                error: {
                    DEFAULT: "#eb5757",
                    light: "#ff7a7a",
                    dark: "#c94040",
                },
                warning: {
                    DEFAULT: "#f2c94c",
                    light: "#f5da7a",
                    dark: "#c9a428",
                },
                info: {
                    DEFAULT: "#26b5ce",
                    light: "#5cd0e3",
                    dark: "#1a8a9c",
                },
                // Linear-style neutral surfaces
                surface: {
                    // light mode
                    base: "#ffffff",
                    subtle: "#fbfbfc",
                    muted: "#f4f5f8",
                    hover: "#eeeff2",
                    active: "#e6e7eb",
                    // dark mode
                    "dark-base": "#08090a",
                    "dark-subtle": "#101113",
                    "dark-muted": "#181a1f",
                    "dark-hover": "#22262f",
                    "dark-active": "#2a2e37",
                },
                border: {
                    subtle: "#e6e7eb",
                    strong: "#d0d3d9",
                    "dark-subtle": "#23252a",
                    "dark-strong": "#33363d",
                },
                text: {
                    primary: "#0d0e10",
                    secondary: "#4b4f57",
                    muted: "#6b7280",
                    "dark-primary": "#f7f8f8",
                    "dark-secondary": "#b4b8c0",
                    "dark-muted": "#8a8f98",
                },
                // Legacy aliases kept so any lingering `bg-dark`/`bg-light` styles still resolve
                dark: {
                    DEFAULT: "#08090a",
                    paper: "#101113",
                    border: "#23252a",
                },
                light: {
                    DEFAULT: "#fbfbfc",
                    paper: "#ffffff",
                    border: "#e6e7eb",
                },
            },
            boxShadow: {
                // Linear-style — extremely subtle, layered, never heavy
                subtle: "0 1px 0 0 rgba(15, 17, 22, 0.04)",
                card: "0 1px 2px 0 rgba(15, 17, 22, 0.04)",
                "card-hover": "0 2px 4px 0 rgba(15, 17, 22, 0.06)",
                popover: "0 4px 12px -2px rgba(15, 17, 22, 0.08), 0 0 0 1px rgba(15, 17, 22, 0.06)",
                overlay: "0 8px 32px -8px rgba(15, 17, 22, 0.16), 0 0 0 1px rgba(15, 17, 22, 0.08)",
                focus: "0 0 0 2px rgba(94, 106, 210, 0.35)",
            },
            borderRadius: {
                none: "0",
                xs: "3px",
                sm: "4px",
                DEFAULT: "6px",
                md: "6px",
                lg: "8px",
                xl: "10px",
                "2xl": "12px",
                "3xl": "16px",
                full: "9999px",
            },
            animation: {
                "fade-in": "fade-in 160ms ease-out forwards",
                "fade-in-up": "fade-in-up 220ms cubic-bezier(0.16, 1, 0.3, 1) forwards",
            },
            keyframes: {
                "fade-in": {
                    "0%": { opacity: "0" },
                    "100%": { opacity: "1" },
                },
                "fade-in-up": {
                    "0%": { opacity: "0", transform: "translateY(4px)" },
                    "100%": { opacity: "1", transform: "translateY(0)" },
                },
            },
        },
    },
    plugins: [],
};
export default config;
