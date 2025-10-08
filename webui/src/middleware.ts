import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(req: NextRequest) {
    const url = req.nextUrl.clone()

    if (url.pathname.startsWith("/api/v1")) {
        const host = process.env.KVCTL_API_HOST || "localhost:9379"
        url.host = host;

        return NextResponse.rewrite(url)
    }

    return NextResponse.next()
}

export const config = {
    matcher: "/api/v1/:path*",
    runtime: 'nodejs',
}
