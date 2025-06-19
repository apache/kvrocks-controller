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
            colors: {
                primary: {
                    DEFAULT: "#1976d2",
                    light: "#42a5f5",
                    dark: "#1565c0",
                    contrastText: "#fff",
                },
                secondary: {
                    DEFAULT: "#9c27b0",
                    light: "#ba68c8",
                    dark: "#7b1fa2",
                    contrastText: "#fff",
                },
                success: {
                    DEFAULT: "#2e7d32",
                    light: "#4caf50",
                    dark: "#1b5e20",
                },
                error: {
                    DEFAULT: "#d32f2f",
                    light: "#ef5350",
                    dark: "#c62828",
                },
                warning: {
                    DEFAULT: "#ed6c02",
                    light: "#ff9800",
                    dark: "#e65100",
                },
                info: {
                    DEFAULT: "#0288d1",
                    light: "#03a9f4",
                    dark: "#01579b",
                },
                dark: {
                    DEFAULT: "#121212",
                    paper: "#1e1e1e",
                    border: "#333333",
                },
                light: {
                    DEFAULT: "#fafafa",
                    paper: "#ffffff",
                    border: "#e0e0e0",
                },
            },
            backgroundImage: {
                "gradient-radial": "radial-gradient(var(--tw-gradient-stops))",
                "gradient-conic":
                    "conic-gradient(from 180deg at 50% 50%, var(--tw-gradient-stops))",
            },
            boxShadow: {
                card: "0 2px 8px rgba(0, 0, 0, 0.06)",
                "card-hover": "0 4px 12px rgba(0, 0, 0, 0.1)",
                sidebar: "2px 0 5px rgba(0, 0, 0, 0.05)",
                "footer-glow": "0 8px 25px rgba(0, 0, 0, 0.15)",
                subtle: "0 2px 5px rgba(0, 0, 0, 0.05)",
                elevated: "0 10px 30px rgba(0, 0, 0, 0.08)",
            },
            transitionProperty: {
                height: "height",
                spacing: "margin, padding",
                footer: "transform, opacity, box-shadow, border-color",
            },
            animation: {
                "fade-in-up": "fade-in-up 0.6s ease-out forwards",
                "scale-in": "scale-in 0.4s ease-out forwards",
            },
            keyframes: {
                "fade-in-up": {
                    "0%": { opacity: "0", transform: "translateY(20px)" },
                    "100%": { opacity: "1", transform: "translateY(0)" },
                },
                "scale-in": {
                    "0%": { opacity: "0", transform: "scale(0.95)" },
                    "100%": { opacity: "1", transform: "scale(1)" },
                },
            },
            borderRadius: {
                "2xl": "16px",
                "3xl": "24px",
                "4xl": "32px",
            },
        },
    },
    plugins: [],
};
export default config;
