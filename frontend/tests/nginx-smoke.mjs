import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { setTimeout } from "node:timers/promises";

const root = fileURLToPath(new URL("../", import.meta.url));
const docker = (...args) =>
  execFileSync("docker", args, { encoding: "utf8", timeout: 120000 }).trim();
const image = readFileSync(
  new URL("../Dockerfile.runtime", import.meta.url),
  "utf8",
).match(/^FROM (\S+)/m)[1];
const container = docker(
  "run",
  "--detach",
  "--rm",
  "--read-only",
  "--tmpfs",
  "/tmp:size=16m",
  "--memory",
  "64m",
  "--cpus",
  "0.5",
  "--publish",
  "127.0.0.1::8080",
  "--add-host",
  "api.cs-app.svc.cluster.local:127.0.0.1",
  "--mount",
  `type=bind,src=${root}nginx.conf,dst=/etc/nginx/nginx.conf,readonly`,
  "--mount",
  `type=bind,src=${root}out,dst=/usr/share/nginx/html,readonly`,
  "--entrypoint",
  "nginx",
  image,
  "-g",
  "daemon off;",
);
try {
  const address = `http://${docker("port", container, "8080/tcp")}`;
  let ready = false;
  for (let attempt = 0; attempt < 30; attempt++) {
    try {
      ready = (
        await fetch(`${address}/frontend-healthz`, {
          signal: AbortSignal.timeout(1000),
        })
      ).ok;
      if (ready) break;
    } catch {
      /* Wait for the isolated container to start. */
    }
    await setTimeout(200);
  }
  assert.ok(ready, "Nginx readiness deadline exceeded");
  for (const [path, status, noStore] of [
    ["/", 200, false],
    ["/frontend-healthz", 200, false],
    ["/research/v1/catalog.json", 200, true],
    ["/research/missing.json", 404, true],
    ["/readyz", 502, true],
    ["/api/v1/snapshots", 502, true],
  ]) {
    const response = await fetch(`${address}${path}`, {
      signal: AbortSignal.timeout(3000),
    });
    assert.equal(response.status, status, path);
    assert.equal(
      response.headers.get("x-content-type-options"),
      "nosniff",
      path,
    );
    assert.equal(response.headers.get("referrer-policy"), "no-referrer", path);
    assert.equal(response.headers.get("x-frame-options"), "DENY", path);
    if (noStore)
      assert.equal(response.headers.get("cache-control"), "no-store", path);
    await response.arrayBuffer();
  }
  console.log(
    "Nginx static, evidence and failed-upstream responses preserve headers",
  );
} finally {
  docker("stop", container);
}
