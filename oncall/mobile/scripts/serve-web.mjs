// Zero-dependency static server with SPA fallback for the exported Expo web build (loopback only).
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, normalize, resolve } from "node:path";

const root = resolve(process.argv[2] ?? "dist");
const port = Number(process.env.PORT ?? 19006);
const types = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".json": "application/json", ".png": "image/png", ".ico": "image/x-icon", ".svg": "image/svg+xml", ".ttf": "font/ttf", ".map": "application/json" };

createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", "http://localhost");
  let file = normalize(join(root, decodeURIComponent(url.pathname)));
  if (!file.startsWith(root)) { res.writeHead(403).end(); return; }
  try {
    if ((await stat(file)).isDirectory()) file = join(file, "index.html");
  } catch {
    file = extname(file) ? file : join(root, "index.html"); // SPA fallback for client-side routes
  }
  try {
    const body = await readFile(file);
    res.writeHead(200, { "Content-Type": types[extname(file)] ?? "application/octet-stream", "Cache-Control": "no-store" }).end(body);
  } catch {
    res.writeHead(404).end("not found");
  }
}).listen(port, "127.0.0.1", () => console.warn(`serving ${root} on http://127.0.0.1:${port}`));
