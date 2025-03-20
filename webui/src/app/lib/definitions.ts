export type footerColumnItem = {
    label: string;
    to?: string;
    href?: string;
}

export type footerColumn = {
    title: string;
    items: footerColumnItem[];
}

export type footerLogo = {
    height: number;
    width: number;
    alt: string;
    src: string;
    href: string;
};

export type footerConfigType = {
    links: footerColumn[];
    logo: footerLogo;
    copyright: string;
}