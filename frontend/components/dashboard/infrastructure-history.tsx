"use client";

import { useEffect, useState } from "react";
import { z } from "zod";
import { getMetadata, metadataPage } from "@/lib/api/client";
import { DataTable } from "@/components/dashboard/data-table";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { EvidenceRow } from "@/lib/schemas/catalog";

type Kind = "snapshots" | "diffs" | "alerts";
export function InfrastructureHistory({ commit }: { commit: string }) {
  const [asset, setAsset] = useState("k8s-owned-router");
  const [submitted, setSubmitted] = useState("k8s-owned-router");
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [range, setRange] = useState({ start: "", end: "" });
  const [kind, setKind] = useState<Kind>("snapshots");
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [page, setPage] = useState<z.infer<typeof metadataPage> | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    setPage(null);
    getMetadata(
      kind,
      submitted,
      cursors[cursors.length - 1],
      range.start,
      range.end,
    )
      .then((value) => {
        if (active) setPage(value);
      })
      .catch((reason) => {
        if (active) setError(String(reason.message));
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [kind, submitted, cursors, range]);
  const rows: EvidenceRow[] = (page?.items ?? []).map((item) => ({
    id: item.snapshot_id ?? item.diff_id ?? item.alert_id ?? item.asset_id,
    label: item.snapshot_id ?? item.diff_id ?? item.alert_id ?? item.asset_id,
    unit: item.version
      ? `configuration version ${item.version}`
      : `${kind} metadata`,
    scope: item.asset_id,
    status: "COMMITTED METADATA",
    source: "PROJECT-OWNED DEMO FIXTURE",
    artifact:
      "workloads/results/phase20-k8s/closure-application-validation.json",
    details: item,
  }));
  return (
    <div className="space-y-6">
      <p className="text-sm text-muted-foreground">
        Live configuration metadata API. The default asset is the accepted owned
        Kubernetes fixture: two versions and one adjacent diff. TerraDS
        aggregates describe the accepted selection; its private full history is
        not exposed here. Git revisions do not establish deployment history.
      </p>
      <form
        className="flex flex-wrap items-end gap-3"
        onSubmit={(event) => {
          event.preventDefault();
          setSubmitted(asset.trim());
          setRange({ start, end });
          setCursors([null]);
        }}
      >
        <div>
          <Label htmlFor="asset" className="mb-1">
            Asset ID (exact match)
          </Label>
          <Input
            id="asset"
            value={asset}
            onChange={(event) => setAsset(event.target.value)}
            maxLength={1024}
            className="w-64"
          />
        </div>
        <div>
          <Label htmlFor="history-start" className="mb-1">
            From UTC date
          </Label>
          <Input
            id="history-start"
            type="date"
            value={start}
            onChange={(event) => setStart(event.target.value)}
          />
        </div>
        <div>
          <Label htmlFor="history-end" className="mb-1">
            Through UTC date
          </Label>
          <Input
            id="history-end"
            type="date"
            value={end}
            onChange={(event) => setEnd(event.target.value)}
          />
        </div>
        <Button type="submit">Apply filters</Button>
      </form>
      <Tabs
        value={kind}
        onValueChange={(value) => {
          setKind(value as Kind);
          setCursors([null]);
        }}
      >
        <TabsList>
          <TabsTrigger value="snapshots">Versions</TabsTrigger>
          <TabsTrigger value="diffs">Adjacent diffs</TabsTrigger>
          <TabsTrigger value="alerts">Alerts</TabsTrigger>
        </TabsList>
        <TabsContent value={kind}>
          {loading ? (
            <p role="status">Loading committed metadata…</p>
          ) : error ? (
            <div role="alert" className="rounded-lg border p-4">
              <p>{error}</p>
              <Button
                variant="outline"
                className="mt-3"
                onClick={() => setCursors([...cursors])}
              >
                Retry API
              </Button>
            </div>
          ) : (
            <>
              <DataTable
                data={rows}
                title="Configuration metadata"
                commit={commit}
              />
              <div className="flex items-center justify-end gap-3">
                <span className="text-xs text-muted-foreground">
                  Server page {cursors.length} · at most 10 records per request
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={cursors.length === 1}
                  onClick={() => setCursors(cursors.slice(0, -1))}
                >
                  Previous server page
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!page?.has_more || !page.next_cursor}
                  onClick={() => setCursors([...cursors, page!.next_cursor])}
                >
                  Next server page
                </Button>
              </div>
            </>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
