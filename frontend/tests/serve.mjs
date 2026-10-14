import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { resolve, extname } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../out/", import.meta.url));
const types = {
  ".html": "text/html",
  ".json": "application/json",
  ".js": "text/javascript",
  ".css": "text/css",
  ".woff2": "font/woff2",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};
createServer(async (request, response) => {
  try {
    if (!["GET", "HEAD"].includes(request.method)) {
      response.writeHead(405, { Allow: "GET, HEAD" }).end();
      return;
    }
    const url = new URL(request.url, "http://127.0.0.1");
    if (
      /^\/api\/v1\/(snapshots|diffs|alerts|assets)(\/|$)/.test(url.pathname) ||
      ["/healthz", "/readyz"].includes(url.pathname)
    ) {
      const upstream = await fetch(
        `http://127.0.0.1:18800${url.pathname}${url.search}`,
        { signal: AbortSignal.timeout(10000) },
      );
      response
        .writeHead(upstream.status, {
          "Content-Type": "application/json",
          "Cache-Control": "no-store",
        })
        .end(
          request.method === "HEAD"
            ? undefined
            : Buffer.from(await upstream.arrayBuffer()),
        );
      return;
    }
    const path = resolve(
      root,
      "." +
        decodeURIComponent(url.pathname) +
        (url.pathname.endsWith("/") ? "index.html" : ""),
    );
    if (!path.startsWith(root)) {
      response.writeHead(404).end();
      return;
    }
    const data = await readFile(path);
    response
      .writeHead(200, {
        "Content-Type": types[extname(path)] ?? "application/octet-stream",
        "Content-Length": data.length,
      })
      .end(request.method === "HEAD" ? undefined : data);
  } catch {
    response
      .writeHead(503, { "Content-Type": "application/json" })
      .end(
        request.method === "HEAD"
          ? undefined
          : JSON.stringify({ error: "Unavailable" }),
      );
  }
}).listen(13000, "127.0.0.1", () =>
  console.log("Static dashboard ready on127.0.0.1:13000"),
);
