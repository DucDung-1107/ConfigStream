"use client";

import { useEffect, useState } from "react";
import { loadCatalog, probeHealth } from "@/lib/api/client";
import { AppSidebar } from "@/components/dashboard/app-sidebar";
import { SiteHeader } from "@/components/dashboard/site-header";
import {
  SectionCards,
  type MetricCard,
} from "@/components/dashboard/section-cards";
import { ChartAreaInteractive } from "@/components/dashboard/chart-area-interactive";
import { ObservationChart } from "@/components/dashboard/observation-chart";
import { DataTable } from "@/components/dashboard/data-table";
import { InfrastructureHistory } from "@/components/dashboard/infrastructure-history";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import {
  Select,
  SelectTrigger,
  SelectContent,
  SelectItem,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  count,
  bytes,
  seconds,
  percentSaving,
  sourceUrl,
} from "@/lib/formatting";
import type { Catalog, EvidenceRow } from "@/lib/schemas/catalog";

export type Screen =
  | "overview"
  | "history"
  | "rpki"
  | "ris"
  | "dns"
  | "experiments"
  | "operations";
const titles: Record<Screen, string> = {
  overview: "Overview",
  history: "Infrastructure History",
  rpki: "RPKI States",
  ris: "RIS Events",
  dns: "DNS Measurements",
  experiments: "Experiments",
  operations: "Pipeline & Operations",
};
function row(
  id: string,
  label: string,
  unit: string,
  scope: string,
  status: string,
  artifact: string,
  details: Record<string, unknown>,
  source = "ACCEPTED REAL-SOURCE AGGREGATE",
): EvidenceRow {
  return {
    id,
    label,
    unit,
    scope,
    status,
    artifact,
    details,
    source,
    role:
      typeof details.semantic_role === "string"
        ? details.semantic_role
        : undefined,
  };
}
function Notice({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-lg border bg-muted/30 px-4 py-3 text-sm leading-relaxed">
      {children}
    </div>
  );
}

export function Dashboard({ screen }: { screen: Screen }) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState("");
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setCatalog(null);
    setError("");
    loadCatalog(controller.signal)
      .then(setCatalog)
      .catch((reason) => {
        if (!controller.signal.aborted) setError(reason.message);
      });
    return () => controller.abort();
  }, [attempt]);
  return (
    <SidebarProvider
      style={
        {
          "--sidebar-width": "calc(var(--spacing) * 72)",
          "--header-height": "calc(var(--spacing) * 12)",
        } as React.CSSProperties
      }
    >
      <AppSidebar variant="inset" />
      <SidebarInset className="min-w-0">
        <SiteHeader title={titles[screen]} />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
              {error ? (
                <div
                  role="alert"
                  className="mx-4 rounded-lg border p-6 lg:mx-6"
                >
                  <h2 className="font-semibold">Evidence unavailable</h2>
                  <p className="mt-2 text-sm">
                    {error}. No previous research values are shown as current.
                  </p>
                  <Button
                    className="mt-4"
                    variant="outline"
                    onClick={() => setAttempt(attempt + 1)}
                  >
                    Retry evidence
                  </Button>
                </div>
              ) : !catalog ? (
                <div className="space-y-4 px-4 lg:px-6" role="status">
                  <p className="text-sm text-muted-foreground">
                    Validating accepted evidence…
                  </p>
                  <Skeleton className="h-36" />
                  <Skeleton className="h-64" />
                </div>
              ) : (
                <Content screen={screen} catalog={catalog} />
              )}
            </div>
          </div>
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}

function Content({ screen, catalog }: { screen: Screen; catalog: Catalog }) {
  const [selectedDay, setSelectedDay] = useState(
    catalog.rpki_days.at(-1)?.date ?? "",
  );
  const [health, setHealth] = useState<{
    status: string;
    timestamp: string;
  } | null>(null);
  const [probing, setProbing] = useState(false);
  useEffect(() => {
    if (!health || health.status !== "READY") return;
    const timer = setTimeout(
      () =>
        setHealth((previous) =>
          previous ? { ...previous, status: "STALE" } : null,
        ),
      60000,
    );
    return () => clearTimeout(timer);
  }, [health?.timestamp, health?.status]);
  const evidence = (artifact: string, label = "Source evidence") => (
    <a
      className="text-xs underline underline-offset-4"
      href={sourceUrl(catalog.artifact_commit, artifact)}
      target="_blank"
      rel="noopener noreferrer"
    >
      {label}
    </a>
  );
  const table = (data: EvidenceRow[], title: string) => (
    <DataTable data={data} title={title} commit={catalog.artifact_commit} />
  );
  let cards: MetricCard[] = [];
  let body: React.ReactNode;
  if (screen === "overview") {
    cards = catalog.domains.map((domain) => ({
      label: domain.name,
      value: domain.count,
      unit: domain.unit,
      scope: domain.scope,
    }));
    body = (
      <>
        <ChartAreaInteractive minutes={catalog.ris_minutes} />
        {table(
          catalog.domains.map((domain) =>
            row(
              domain.name,
              domain.name,
              `${count(domain.count)} ${domain.unit}`,
              domain.scope,
              domain.status,
              domain.artifact,
              {
                semantic_role: domain.role,
                qualification: domain.qualifier,
                source_field: domain.field,
              },
            ),
          ),
          "Accepted workload scopes",
        )}
        <Notice>
          Each workload uses a different native unit. The chart shows only RIS
          occurrences; these four card values cannot be summed into an
          infrastructure-change count.
        </Notice>
        <Card>
          <CardHeader>
            <CardTitle>Data Sources & Evidence</CardTitle>
            <CardDescription>
              Approved aggregates are staged independently of private source
              availability.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-2">
            {catalog.attribution.map((source) => (
              <div key={source.name}>
                <a
                  className="text-sm font-medium underline"
                  href={source.url}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  {source.name}
                </a>
                <p className="mt-1 text-xs text-muted-foreground">
                  {source.scope}
                </p>
              </div>
            ))}
          </CardContent>
        </Card>
      </>
    );
  } else if (screen === "history") {
    const domain = catalog.domains[0];
    cards = [
      {
        label: "Accepted TerraDS history",
        value: domain.count,
        unit: domain.unit,
        scope: "Recorded aggregate · live demo is the owned fixture",
      },
      {
        label: "Owned demo versions",
        value: catalog.operations.fixture_versions,
        unit: "configuration versions",
        scope: "Recorded Kubernetes acceptance",
      },
      {
        label: "Owned adjacent comparison",
        value: catalog.operations.fixture_diffs,
        unit: "adjacent diff",
        scope: "Recorded Kubernetes acceptance",
      },
    ];
    body = <InfrastructureHistory commit={catalog.artifact_commit} />;
  } else if (screen === "rpki") {
    const selected = catalog.rpki_days.find((day) => day.date === selectedDay)!;
    cards = [
      {
        label: "Selected observed membership",
        value: selected.members,
        unit: "canonical members",
        scope: `ripencc.tal · ${selected.date}`,
      },
      {
        label: "Observed additions",
        value: selected.additions,
        unit: "members at adjacent boundary",
        scope:
          selected.additions === null
            ? "No previous selected day"
            : "Complete consecutive scoped observations",
      },
      {
        label: "Observed removals",
        value: selected.removals,
        unit: "members at adjacent boundary",
        scope:
          selected.removals === null
            ? "No previous selected day"
            : "Complete consecutive scoped observations",
      },
      {
        label: "Accepted complete days",
        value: catalog.rpki_days.length,
        unit: "daily observations",
        scope: "2026-09-01 through 2026-09-14",
      },
    ];
    body = (
      <>
        <div className="flex flex-wrap items-center gap-3">
          <span className="text-sm font-medium">
            Select complete observation
          </span>
          <Select value={selectedDay} onValueChange={setSelectedDay}>
            <SelectTrigger aria-label="RPKI observation day" className="w-44">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {catalog.rpki_days.map((day) => (
                <SelectItem key={day.date} value={day.date}>
                  {day.date}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Badge variant="outline">COMPLETE · recorded</Badge>
          {evidence(selected.artifact)}
        </div>
        <ObservationChart
          title="Scoped daily observed membership"
          description="ripencc.tal · accepted complete canonical sets"
          points={catalog.rpki_days.map((day) => ({
            date: day.date.slice(5),
            value: day.members,
          }))}
          unit="canonical members"
        />
        {table(
          catalog.rpki_days.map((day) =>
            row(
              day.observation_id,
              day.date,
              `${count(day.members)} members`,
              "ripencc.tal",
              day.status,
              day.artifact,
              {
                ...day,
                interpretation:
                  "Observed authorization memberships; additions/removals are not administrator operations.",
              },
            ),
          ),
          "Complete daily observations",
        )}
        <Notice>
          Completeness binds the selected trust-anchor observation. An
          incomplete source day cannot produce ordinary removal transitions.
          Every accepted day was independently reconstructed; select a row to
          inspect its set digest.
        </Notice>
      </>
    );
  } else if (screen === "ris") {
    const summary = catalog.ris_summary;
    cards = [
      {
        label: "Native occurrences",
        value: Number(summary.occurrences),
        unit: "accepted peer-prefix occurrences",
        scope: "rrc00 · selected source hour",
      },
      {
        label: "Announcements",
        value: Number(summary.announcements),
        unit: "native occurrences",
        scope: "Historical UPDATE evidence",
      },
      {
        label: "Withdrawals",
        value: Number(summary.withdrawals),
        unit: "native occurrences",
        scope: "Historical UPDATE evidence",
      },
      {
        label: "Parser exclusions",
        value: Number(summary.exclusions),
        unit: "fully framed source records",
        scope: "Durable independent error accounting",
      },
    ];
    body = (
      <>
        <ChartAreaInteractive minutes={catalog.ris_minutes} />
        <Notice>
          {count(Number(summary.repeated_fingerprints))} repeated descriptive
          fingerprints beyond first remain in the accepted native history. Only
          identical source identities reconcile as transport replay. Full-hour
          acceptance used retained-history recovery; UPDATE archives do not
          reconstruct complete routing state.
        </Notice>
        <Tabs defaultValue="minutes">
          <TabsList>
            <TabsTrigger value="minutes">Source minutes</TabsTrigger>
            <TabsTrigger value="fixtures">
              Owned occurrence identities
            </TabsTrigger>
          </TabsList>
          <TabsContent value="minutes" className="mt-4">
            {table(
              catalog.ris_minutes.map((minute) =>
                row(
                  minute.time,
                  `${minute.time.slice(11, 16)} UTC`,
                  `${count(minute.occurrences)} occurrences`,
                  "rrc00 · 2026-09-01",
                  "ACCEPTED",
                  minute.artifact,
                  minute,
                ),
              ),
              "Accepted minute activity",
            )}
          </TabsContent>
          <TabsContent value="fixtures" className="mt-4">
            <p className="mb-4 text-sm text-muted-foreground">
              Project-owned demo: four source occurrences and two content
              fingerprints. Equal-looking record bytes at distinct MRT offsets
              retain distinct identities.
            </p>
            {table(
              catalog.ris_fixture.map((item) =>
                row(
                  item.id,
                  `${item.action} · ${item.prefix}`,
                  `offset ${item.offset}, ordinal ${item.ordinal}`,
                  "Owned MRT fixture",
                  "DISTINCT OCCURRENCE",
                  item.artifact,
                  item,
                  item.evidence,
                ),
              ),
              "Owned routing occurrence details",
            )}
          </TabsContent>
        </Tabs>
      </>
    );
  } else if (screen === "dns") {
    const comparison = catalog.dns_comparison;
    cards = [
      {
        label: "Common address groups",
        value: Number(comparison.common_groups),
        unit: "groups in both selected objects",
        scope: "Frozen descriptive grouping policy",
      },
      {
        label: "Differing observed values",
        value: Number(comparison.changed_groups),
        unit: "groups with differing address sets",
        scope: "Causal interpretation unknown",
      },
      {
        label: "Not observed on day two",
        value: Number(comparison.no_longer_observed_groups),
        unit: "previously observed groups",
        scope: "No authoritative deletion inference",
      },
      {
        label: "Strict matched-vantage eligible",
        value: Number(comparison.strict_matched_vantage_eligible),
        unit: "proven comparable groups",
        scope: "Resolver/vantage not independently known",
      },
    ];
    body = (
      <>
        <Notice>
          Differing observed addresses do not prove authoritative DNS edits. A
          complete selected object does not establish complete entity coverage.
          Missing measurement, timeout, failure and unknown status never imply
          an empty answer.
        </Notice>
        <ObservationChart
          title="Daily canonical observed address groups"
          description="Two selected OpenINTEL .li zonefile-basis Parquet objects"
          points={catalog.dns_days.map((day) => ({
            date: String(day.date),
            value: Number(day.groups),
          }))}
          unit="observed address groups"
        />
        {table(
          catalog.dns_days.map((day) =>
            row(
              String(day.date),
              String(day.date),
              `${count(Number(day.rows))} raw measurements`,
              "Two selected .li objects",
              "ENTITY COVERAGE UNKNOWN",
              String(day.artifact),
              {
                ...day,
                timestamp_encoding: "epoch millisecond",
                source_precision: "second",
                worker_qualification:
                  "Recorded worker IDs do not prove stable resolver/vantage",
                proven_empty_evidence:
                  "No real proven-empty measurements established",
              },
            ),
          ),
          "Selected DNS object accounting",
        )}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Descriptive observed-value comparison</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <p>
                {count(Number(comparison.added_values))} observed added values;{" "}
                {count(Number(comparison.removed_values))} observed removed
                values.
              </p>
              <p>
                {count(Number(comparison.new_observed_groups))} newly observed
                groups; {count(Number(comparison.no_longer_observed_groups))}{" "}
                groups not observed in the second selected object.
              </p>
              {evidence(String(comparison.artifact))}
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Measurement qualification</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <p>
                Status 0: successful response; 2/3: unsuccessful; 65533:
                explicitly unknown. Real timeout code 65535 did not occur in
                these selected objects.
              </p>
              <p>
                Epoch-millisecond encoding has declared second precision.
                Recorded workers do not prove matched vantage. Successful empty
                needs explicit query evidence; absence of A/AAAA rows is
                insufficient.
              </p>
              <p>{String(comparison.strict_reason)}</p>
            </CardContent>
          </Card>
        </div>
      </>
    );
  } else if (screen === "experiments") {
    const baseline = Number(
      catalog.representations.find((item) => item.mode === "R1")!.bytes,
    );
    cards = catalog.representations.map((item) => ({
      label: `${item.mode} analytical representation`,
      value: Number(item.bytes),
      unit: "physical analytical bytes",
      scope:
        item.mode === "R1"
          ? "Full canonical states"
          : item.mode === "R2"
            ? "Baseline + exact adjacent deltas"
            : "Seven-day checkpoints + exact deltas",
    }));
    body = (
      <>
        <Notice>
          Phase 20 remains CONDITIONAL. RPKI matched analytical representations
          passed the registered 20% saving / ≤2× per-query median preference
          rule. All-in retained storage includes source/accounting/scratch
          components. RIS H3 physical 1× replay remains NOT_RUN.
        </Notice>
        <Tabs defaultValue="storage">
          <TabsList>
            <TabsTrigger value="storage">Storage & queries</TabsTrigger>
            <TabsTrigger value="processing">Matched processing</TabsTrigger>
            <TabsTrigger value="findings">Negative findings</TabsTrigger>
          </TabsList>
          <TabsContent value="storage" className="mt-4 space-y-6">
            {table(
              catalog.representations.map((item) =>
                row(
                  String(item.mode),
                  String(item.mode),
                  bytes(Number(item.bytes)),
                  `14 RPKI days · all-in component median ${count(Number(item.all_in_bytes))} bytes`,
                  "EXACT RECONSTRUCTION",
                  String(item.artifact),
                  {
                    analytical_bytes: item.bytes,
                    analytical_saving_percent: percentSaving(
                      baseline,
                      Number(item.bytes),
                    ).toFixed(2),
                    all_in_component_median_bytes: item.all_in_bytes,
                    all_in_artifact: item.all_in_artifact,
                    accounting:
                      "Shared source counted once; deployed Kafka/checkpoint costs require their separate storage inventory",
                  },
                ),
              ),
              "RPKI representation costs",
            )}
            {table(
              catalog.queries.map((item) =>
                row(
                  `${item.mode}-${item.query}`,
                  `${item.mode} · ${String(item.query).replaceAll("_", " ")}`,
                  `${seconds(Number(item.median))} median · ${seconds(Number(item.minimum))}–${seconds(Number(item.maximum))} range`,
                  "Three build medians; warm cycles; OS cache uncontrolled",
                  String(item.status),
                  String(item.artifact),
                  item,
                ),
              ),
              "Registered reconstruction/query measurements",
            )}
          </TabsContent>
          <TabsContent value="processing" className="mt-4 space-y-6">
            <p className="text-sm text-muted-foreground">
              B0 direct Parquet, B2 Spark batch, B3 Kafka→Spark. Same accepted
              normalized input projection; parser and production state/group
              finalization excluded. Command-start to observed exit is the
              displayed timing. Three repetitions per configuration; no
              universal engine ranking.
            </p>
            {table(
              catalog.processing.map((item) =>
                row(
                  `${item.workload}-${item.mode}-${item.repetition}`,
                  `${String(item.workload).toUpperCase()} · ${item.mode} · repeat ${item.repetition}`,
                  seconds(Number(item.seconds)),
                  `${count(Number(item.rows))} normalized rows`,
                  String(item.status),
                  String(item.artifact),
                  item,
                  "MEASURED NORMALIZED-CORE EXPERIMENT",
                ),
              ),
              "Matched normalized-core trials",
            )}
          </TabsContent>
          <TabsContent value="findings" className="mt-4 space-y-4">
            {catalog.negative_findings.map((item) => (
              <Card key={String(item.label)}>
                <CardHeader>
                  <CardTitle>{String(item.label)}</CardTitle>
                  <CardDescription>
                    {item.value === null
                      ? String(item.unit)
                      : `${count(Number(item.value))} ${item.unit}`}
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-3 text-sm">
                  <p>{String(item.qualifier)}</p>
                  {evidence(String(item.artifact))}
                </CardContent>
              </Card>
            ))}
          </TabsContent>
        </Tabs>
        <div className="grid gap-4 md:grid-cols-3">
          {Object.entries(catalog.hypotheses).map(([name, hypothesis]) => (
            <Card key={name}>
              <CardHeader>
                <CardTitle>{name}</CardTitle>
                <CardDescription className="break-words">
                  {hypothesis.decision.replaceAll("_", " ")}
                </CardDescription>
              </CardHeader>
              <CardContent className="text-xs text-muted-foreground">
                {hypothesis.finding ?? hypothesis.limit}
              </CardContent>
            </Card>
          ))}
        </div>
      </>
    );
  } else {
    const ops = catalog.operations;
    cards = [
      {
        label: "Recorded Kubernetes topology",
        value: ops.node_count,
        unit: "ARM64 node",
        scope: `${ops.k3s} · ${ops.recorded_date}`,
      },
      {
        label: "Recorded fixture versions",
        value: ops.fixture_versions,
        unit: "accepted logical identities",
        scope: "Recorded native Kafka/Spark/MinIO run",
      },
      {
        label: "Recorded selected Kafka lag",
        value: ops.selected_lag,
        unit: "selected-offset lag",
        scope: `Recorded ${ops.recorded_date}; live lag unavailable`,
      },
      {
        label: "Recorded resource sample",
        value: ops.peak_sampled_working_set_bytes,
        unit: "sampled cs-* working-set bytes",
        scope: "Historical sample; instantaneous total peak unknown",
      },
    ];
    body = (
      <>
        <Card>
          <CardHeader>
            <CardTitle>Serving health</CardTitle>
            <CardDescription>
              Fresh authorized API readiness request; other service metrics
              remain recorded.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap items-center gap-4 text-sm">
            <Button
              variant="outline"
              disabled={probing}
              onClick={async () => {
                setProbing(true);
                setHealth(null);
                setHealth(await probeHealth());
                setProbing(false);
              }}
            >
              {probing ? "Checking…" : "Check API now"}
            </Button>
            <span role="status">
              {health
                ? `${health.status === "STALE" ? "STALE previous probe" : `LIVE ${health.status}`} · request ${health.timestamp}`
                : "NOT AVAILABLE · no fresh probe requested"}
            </span>
          </CardContent>
        </Card>
        <Notice>
          Kafka → Spark Streaming → committed MinIO/Parquet → Spark Batch →
          Elasticsearch/FastAPI → Dashboard. Search/API serve verified
          configuration metadata only; RPKI/RIS/DNS screens use approved
          source-bound aggregates.
        </Notice>
        {table(
          ops.native_pods.map((pod) =>
            row(
              pod.uid,
              pod.name,
              pod.role,
              "Native Kubernetes Spark",
              `RECORDED ${pod.phase.toUpperCase()}`,
              ops.native_artifact,
              pod,
              "RECORDED KUBERNETES RUN",
            ),
          ),
          "Native driver/executor evidence",
        )}
        <Card>
          <CardHeader>
            <CardTitle>Recovery and deployment boundaries</CardTitle>
            <CardDescription>
              RECORDED PASS · bounded owned fixtures · {ops.recorded_date}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <p>
              {ops.recovery}. Safe Kafka/MinIO/ES pod replacements and
              commit-before-checkpoint restart preserved accepted history.
            </p>
            <p>{ops.limits}</p>
            <p>{ops.measurement}</p>
            <p>
              Recorded VM: {ops.vm.cpus} CPUs / {ops.vm.memory_gib} GiB RAM /{" "}
              {ops.vm.disk_gib} GiB disk.
            </p>
            <p>
              Live Kafka lag and live cluster-wide resource metrics are not
              exposed by this read-only dashboard.
            </p>
            <div className="flex flex-wrap gap-4">
              {evidence(ops.artifact, "Application receipt")}
              {evidence(ops.recovery_artifact, "Recovery receipt")}
              {evidence(ops.resource_artifact, "Resource receipt")}
            </div>
          </CardContent>
        </Card>
      </>
    );
  }
  return (
    <>
      {cards.length > 0 && <SectionCards cards={cards} />}
      <div className="space-y-6 px-4 lg:px-6">
        {body}
        <footer className="border-t pt-4 text-xs leading-relaxed text-muted-foreground">
          Verified evidence export built {catalog.generated_at}. Historical
          source windows and live request timestamps are distinct. Source
          binding {catalog.artifact_commit.slice(0, 12)} · immutable artifact
          hashes validated before display.
          <div className="mt-2">
            UI foundation:{" "}
            <a
              className="underline"
              href="https://ui.shadcn.com/blocks#dashboard-01"
              target="_blank"
              rel="noopener noreferrer"
            >
              shadcn/ui dashboard-01
            </a>{" "}
            ·{" "}
            <a className="underline" href="/licenses/shadcn-ui.txt">
              MIT notice
            </a>
          </div>
        </footer>
      </div>
    </>
  );
}
