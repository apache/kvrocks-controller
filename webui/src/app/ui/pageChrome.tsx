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

import { ReactNode, useState } from "react";
import { IconButton, Menu, MenuItem, Tooltip } from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import FilterListIcon from "@mui/icons-material/FilterList";
import SortIcon from "@mui/icons-material/Sort";
import CloseIcon from "@mui/icons-material/Close";
import CheckIcon from "@mui/icons-material/Check";

export interface PageShellProps {
    sidebar?: ReactNode;
    children: ReactNode;
}

export function PageShell({ sidebar, children }: PageShellProps) {
    return (
        <div className="mx-auto flex w-full max-w-[1440px] items-stretch">
            {sidebar}
            <div className="lin-fade-in min-w-0 flex-1">{children}</div>
        </div>
    );
}

export function PageBody({ children }: { children: ReactNode }) {
    return <div className="px-8 py-8 md:px-10 md:py-10">{children}</div>;
}

export interface PageHeaderProps {
    title: ReactNode;
    subtitle?: ReactNode;
    icon?: ReactNode;
    actions?: ReactNode;
}

export function PageHeader({ title, subtitle, icon, actions }: PageHeaderProps) {
    return (
        <header className="flex flex-col gap-4 border-b border-border-subtle px-8 py-7 dark:border-border-dark-subtle md:flex-row md:items-center md:justify-between md:px-10 md:py-8">
            <div className="flex items-start gap-4">
                {icon && (
                    <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-lg bg-surface-muted text-text-secondary dark:bg-surface-dark-muted dark:text-text-dark-secondary">
                        {icon}
                    </div>
                )}
                <div className="min-w-0">
                    <h1 className="truncate text-xl font-semibold tracking-tight text-text-primary dark:text-text-dark-primary">
                        {title}
                    </h1>
                    {subtitle && (
                        <p className="mt-1 text-sm text-text-muted dark:text-text-dark-muted">
                            {subtitle}
                        </p>
                    )}
                </div>
            </div>
            {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
        </header>
    );
}

export interface StatCardProps {
    label: string;
    value: number | string;
    icon?: ReactNode;
    accent?: "default" | "primary" | "success" | "warning" | "info";
}

const accentClasses: Record<NonNullable<StatCardProps["accent"]>, string> = {
    default:
        "text-text-muted bg-surface-muted dark:text-text-dark-muted dark:bg-surface-dark-muted",
    primary: "text-primary bg-primary/10 dark:text-primary-light dark:bg-primary/15",
    success: "text-success bg-success/10 dark:bg-success/15",
    warning: "text-warning bg-warning/10 dark:bg-warning/15",
    info: "text-info bg-info/10 dark:bg-info/15",
};

export function StatCard({ label, value, icon, accent = "default" }: StatCardProps) {
    return (
        <div className="flex items-center gap-4 rounded-xl border border-border-subtle bg-surface-base p-5 shadow-subtle transition-colors hover:border-border-strong dark:border-border-dark-subtle dark:bg-surface-dark-subtle dark:hover:border-border-dark-strong">
            {icon && (
                <div
                    className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-lg ${accentClasses[accent]}`}
                >
                    {icon}
                </div>
            )}
            <div className="min-w-0">
                <div className="text-xs uppercase tracking-wider text-text-muted dark:text-text-dark-muted">
                    {label}
                </div>
                <div className="mt-1 text-2xl font-semibold tracking-tight text-text-primary dark:text-text-dark-primary">
                    {value}
                </div>
            </div>
        </div>
    );
}

export interface SearchInputProps {
    value: string;
    onChange: (v: string) => void;
    placeholder?: string;
}

export function SearchInput({ value, onChange, placeholder = "Search…" }: SearchInputProps) {
    return (
        <div className="relative w-full md:w-80">
            <SearchIcon
                sx={{ fontSize: 16 }}
                className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-text-muted dark:text-text-dark-muted"
            />
            <input
                type="text"
                value={value}
                onChange={(e) => onChange(e.target.value)}
                placeholder={placeholder}
                className="h-10 w-full rounded-lg border border-border-subtle bg-surface-base pl-10 pr-9 text-sm text-text-primary transition-colors placeholder:text-text-muted hover:border-border-strong focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20 dark:border-border-dark-subtle dark:bg-surface-dark-subtle dark:text-text-dark-primary dark:placeholder:text-text-dark-muted"
            />
            {value && (
                <button
                    type="button"
                    aria-label="Clear search"
                    onClick={() => onChange("")}
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 rounded p-1 text-text-muted hover:bg-surface-hover hover:text-text-primary dark:text-text-dark-muted dark:hover:bg-surface-dark-hover dark:hover:text-text-dark-primary"
                >
                    <CloseIcon sx={{ fontSize: 14 }} />
                </button>
            )}
        </div>
    );
}

export interface FilterSortOption<T extends string> {
    value: T;
    label: string;
    group?: string;
}

export interface FilterSortMenuProps<T extends string> {
    ariaLabel: string;
    icon: ReactNode;
    tooltip: string;
    value: T;
    options: FilterSortOption<T>[];
    onChange: (v: T) => void;
}

export function FilterSortMenu<T extends string>({
    ariaLabel,
    icon,
    tooltip,
    value,
    options,
    onChange,
}: FilterSortMenuProps<T>) {
    const [anchor, setAnchor] = useState<HTMLElement | null>(null);

    // Group options while preserving order.
    const groups: { title: string | null; items: FilterSortOption<T>[] }[] = [];
    for (const opt of options) {
        const g = groups.find((x) => x.title === (opt.group ?? null));
        if (g) g.items.push(opt);
        else groups.push({ title: opt.group ?? null, items: [opt] });
    }

    return (
        <>
            <Tooltip title={tooltip} arrow>
                <IconButton
                    size="small"
                    onClick={(e) => setAnchor(e.currentTarget)}
                    aria-label={ariaLabel}
                    sx={{ width: 34, height: 34 }}
                >
                    {icon}
                </IconButton>
            </Tooltip>
            <Menu
                open={!!anchor}
                anchorEl={anchor}
                onClose={() => setAnchor(null)}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                transformOrigin={{ vertical: "top", horizontal: "right" }}
                PaperProps={{ sx: { minWidth: 200 } }}
            >
                {groups.map((group, gi) => [
                    group.title ? (
                        <li key={`g-${gi}`} className="lin-eyebrow px-3 pb-1 pt-2" aria-hidden>
                            {group.title}
                        </li>
                    ) : null,
                    ...group.items.map((opt) => (
                        <MenuItem
                            key={opt.value}
                            selected={opt.value === value}
                            onClick={() => {
                                onChange(opt.value);
                                setAnchor(null);
                            }}
                        >
                            <span className="flex-1">{opt.label}</span>
                            {opt.value === value && (
                                <CheckIcon sx={{ fontSize: 13, ml: 1 }} className="text-primary" />
                            )}
                        </MenuItem>
                    )),
                ])}
            </Menu>
        </>
    );
}

export interface ResourceRowProps {
    icon: ReactNode;
    title: ReactNode;
    subtitle?: ReactNode;
    badges?: ReactNode;
    meta?: ReactNode;
    href?: string;
    onDelete?: () => void;
    deleteDisabled?: boolean;
    children?: ReactNode;
}

import Link from "next/link";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";

export function ResourceRow({
    icon,
    title,
    subtitle,
    badges,
    meta,
    href,
    onDelete,
    deleteDisabled,
    children,
}: ResourceRowProps) {
    const body = (
        <div className="flex items-center gap-4">
            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-surface-muted text-text-secondary dark:bg-surface-dark-muted dark:text-text-dark-secondary">
                {icon}
            </div>
            <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-2">
                    <span className="truncate text-sm font-semibold text-text-primary dark:text-text-dark-primary">
                        {title}
                    </span>
                    {badges}
                </div>
                {subtitle && (
                    <div className="mt-1 truncate text-sm text-text-muted dark:text-text-dark-muted">
                        {subtitle}
                    </div>
                )}
                {children}
            </div>
            {meta && <div className="hidden shrink-0 md:flex md:items-center md:gap-5">{meta}</div>}
            <div className="flex shrink-0 items-center gap-1">
                {onDelete && (
                    <IconButton
                        size="small"
                        onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            onDelete();
                        }}
                        disabled={deleteDisabled}
                        aria-label="Delete"
                        sx={{
                            width: 32,
                            height: 32,
                            "&:hover": { color: "error.main" },
                        }}
                    >
                        <DeleteOutlineIcon sx={{ fontSize: 17 }} />
                    </IconButton>
                )}
                {href && (
                    <ChevronRightIcon
                        sx={{ fontSize: 18 }}
                        className="text-text-muted dark:text-text-dark-muted"
                    />
                )}
            </div>
        </div>
    );

    const rowClasses =
        "block px-6 py-4 transition-colors hover:bg-surface-hover dark:hover:bg-surface-dark-hover";
    return href ? (
        <Link href={href} className={rowClasses}>
            {body}
        </Link>
    ) : (
        <div className={rowClasses}>{body}</div>
    );
}

// Icon aliases so pages don't need to import twice.
export { FilterListIcon, SortIcon };
