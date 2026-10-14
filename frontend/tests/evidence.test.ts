import { describe, it, expect, vi, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import { catalogSchema } from "@/lib/schemas/catalog";
import { loadCatalog, metadataPage } from "@/lib/api/client";
import { percentSaving, sourceUrl } from "@/lib/formatting";

const raw = readFileSync(
  new URL("../public/research/v1/catalog.json", import.meta.url),
);
const accepted = JSON.parse(raw.toString());
afterEach(() => vi.unstubAllGlobals());
describe("declared evidence and uncertainty", () => {
  it("preserves four native units and accepted source counts", () => {
    const data = catalogSchema.parse(accepted);
    expect(data.domains.map((row) => row.count)).toEqual([
      10188, 14, 4725156, 2234863,
    ]);
    expect(new Set(data.domains.map((row) => row.unit)).size).toBe(4);
  });
  it("rejects an incomplete RPKI day", () => {
    const data = structuredClone(accepted);
    data.rpki_days[0].status = "PARTIAL";
    expect(() => catalogSchema.parse(data)).toThrow();
  });
  it("preserves equal-fingerprint native occurrences", () => {
    const data = catalogSchema.parse(accepted);
    expect(new Set(data.ris_fixture.map((row) => row.id)).size).toBe(4);
    expect(new Set(data.ris_fixture.map((row) => row.fingerprint)).size).toBe(
      2,
    );
    expect(
      data.ris_fixture.every(
        (row) => row.evidence === "PROJECT-OWNED DEMO FIXTURE",
      ),
    ).toBe(true);
  });
  it("rejects promotion of unknown DNS vantage", () => {
    const data = structuredClone(accepted);
    data.dns_comparison.strict_matched_vantage_eligible = 1;
    expect(() => catalogSchema.parse(data)).toThrow();
  });
  it("rejects promotion of unexecuted H3", () => {
    const data = structuredClone(accepted);
    data.hypotheses.H3.decision = "SUPPORTED_WITHIN_TESTED_SCOPE";
    expect(() => catalogSchema.parse(data)).toThrow();
  });
  it("rejects missing source minute", () => {
    const data = structuredClone(accepted);
    data.ris_minutes.pop();
    expect(() => catalogSchema.parse(data)).toThrow();
  });
  it("computes the registered analytical savings from byte counts", () => {
    expect(percentSaving(47756423, 3555516).toFixed(2)).toBe("92.55");
    expect(percentSaving(47756423, 6954890).toFixed(2)).toBe("85.44");
  });
  it("retains DNS negative storage and unknown qualification", () => {
    expect(accepted.negative_findings[0].value).toBe(2207468);
    expect(accepted.dns_comparison.changed_groups).toBe(4091);
    expect(accepted.dns_comparison.no_longer_observed_groups).toBe(676);
    expect(
      accepted.dns_days.every(
        (row: { entity_coverage: string; proven_empty: number }) =>
          row.entity_coverage === "unknown" && row.proven_empty === 0,
      ),
    ).toBe(true);
  });
  it("only constructs approved source links", () => {
    expect(
      sourceUrl(accepted.artifact_commit, accepted.domains[0].artifact),
    ).toContain("/blob/");
    expect(() => sourceUrl(accepted.artifact_commit, "../../.env")).toThrow();
  });
  it("rejects unrestricted API output and raw content", () => {
    expect(() =>
      metadataPage.parse({
        items: Array(201).fill({ asset_id: "fixture" }),
        count: 201,
        has_more: false,
        next_cursor: null,
      }),
    ).toThrow();
    expect(() =>
      metadataPage.parse({
        items: [{ asset_id: "fixture", content: "secret" }],
        count: 1,
        has_more: false,
        next_cursor: null,
      }),
    ).toThrow();
  });
  it("accepts exact immutable bytes before display", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(raw)));
    expect((await loadCatalog()).domains).toHaveLength(4);
  });
  it("rejects corrupted bytes before display", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(raw.toString() + " ")),
    );
    await expect(loadCatalog()).rejects.toThrow("checksum mismatch");
  });
  it("reports unavailable evidence without substituting old data", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("missing", { status: 503 })),
    );
    await expect(loadCatalog()).rejects.toThrow("unavailable");
  });
});
