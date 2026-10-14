import { z } from "zod";
import { catalogSchema } from "@/lib/schemas/catalog";
import { evidenceDigest } from "@/lib/evidence-digest";

export async function loadCatalog(signal?: AbortSignal) {
  const response = await fetch("/research/v1/catalog.json", {
    signal,
    cache: "no-store",
  });
  if (!response.ok)
    throw new Error(`Evidence bundle unavailable (${response.status})`);
  const raw = await response.arrayBuffer();
  if (raw.byteLength > 200000)
    throw new Error("Evidence bundle exceeds bounded contract");
  const hash = Array.from(
    new Uint8Array(await crypto.subtle.digest("SHA-256", raw)),
  )
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
  if (hash !== evidenceDigest)
    throw new Error("Evidence checksum mismatch; research data rejected");
  return catalogSchema.parse(JSON.parse(new TextDecoder().decode(raw)));
}

const metadata = z
  .object({
    snapshot_id: z.string().optional(),
    diff_id: z.string().optional(),
    alert_id: z.string().optional(),
    asset_id: z.string(),
    version: z.number().int().optional(),
    old_snapshot_id: z.string().nullable().optional(),
    new_snapshot_id: z.string().optional(),
    event_time: z.string().optional(),
    created_at: z.string().optional(),
    ingest_time: z.string().optional(),
    raw_hash: z.string().optional(),
    normalized_hash: z.string().optional(),
    normalization_version: z.string().optional(),
    added: z.number().optional(),
    removed: z.number().optional(),
    changed: z.number().optional(),
    severity: z.string().optional(),
    rule_id: z.string().optional(),
    message: z.string().optional(),
    schema_version: z.string().optional(),
  })
  .strict();
export const metadataPage = z
  .object({
    items: z.array(metadata).max(200),
    count: z.number().int().nonnegative(),
    has_more: z.boolean(),
    next_cursor: z.string().nullable(),
  })
  .strict();
export async function getMetadata(
  kind: "snapshots" | "diffs" | "alerts",
  asset: string,
  cursor: string | null,
  start: string,
  end: string,
) {
  const query = new URLSearchParams({ limit: "10" });
  if (asset) query.set("asset_id", asset);
  if (cursor) query.set("cursor", cursor);
  if (start) query.set("start", `${start}T00:00:00Z`);
  if (end) query.set("end", `${end}T23:59:59Z`);
  const response = await fetch(`/api/v1/${kind}?${query}`, {
    cache: "no-store",
  });
  if (!response.ok)
    throw new Error(`Configuration metadata unavailable (${response.status})`);
  return metadataPage.parse(await response.json());
}

export async function probeHealth() {
  const timestamp = new Date().toISOString();
  try {
    const response = await fetch("/readyz", {
      cache: "no-store",
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) throw new Error(String(response.status));
    z.object({ status: z.literal("ok") })
      .strict()
      .parse(await response.json());
    return { status: "READY", timestamp };
  } catch {
    return { status: "UNAVAILABLE", timestamp };
  }
}
