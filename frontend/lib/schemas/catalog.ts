import { z } from "zod";

const artifact = z
  .string()
  .regex(
    /^(workloads\/results\/[a-z0-9-]+\/[a-z0-9-]+\.json|tests\/fixtures\/phase16\/mrt\.json)$/,
  );
const numeric = z.number().finite().nonnegative();
const scalar = z.union([
  z.string(),
  z.number().finite(),
  z.boolean(),
  z.null(),
]);
const row = z.record(
  z.string(),
  z.union([scalar, z.array(scalar), z.record(z.string(), scalar)]),
);
export const domainSchema = z
  .object({
    name: z.enum(["Terraform", "RPKI", "RIS", "DNS"]),
    role: z.string(),
    count: numeric.int(),
    unit: z.string(),
    scope: z.string(),
    artifact,
    field: z.string(),
    qualifier: z.string(),
    evidence: z.literal("ACCEPTED REAL-SOURCE AGGREGATE"),
    status: z.literal("ACCEPTED"),
  })
  .strict();
const day = z
  .object({
    date: z.string(),
    members: numeric.int(),
    status: z.literal("COMPLETE"),
    additions: numeric.int().nullable(),
    removals: numeric.int().nullable(),
    observation_id: z.string(),
    set_digest: z.string(),
    reconstruction_seconds: numeric,
    artifact,
    transition_artifact: artifact,
  })
  .strict();
const minute = z
  .object({
    time: z.iso.datetime({ offset: true }),
    announcement: numeric.int(),
    withdrawal: numeric.int(),
    occurrences: numeric.int(),
    artifact,
  })
  .strict();
const fixture = z
  .object({
    id: z.string().regex(/^[a-f0-9]{64}$/),
    offset: numeric.int(),
    ordinal: numeric.int(),
    fingerprint: z.string().regex(/^[a-f0-9]{64}$/),
    prefix: z.string(),
    action: z.enum(["announcement", "withdrawal"]),
    evidence: z.literal("PROJECT-OWNED DEMO FIXTURE"),
    artifact,
  })
  .strict();
export const catalogSchema = z
  .object({
    schema_version: z.literal("dashboard-1"),
    generated_at: z.iso.datetime({ offset: true }),
    artifact_commit: z.string().regex(/^[a-f0-9]{40}$/),
    artifacts: z.record(
      artifact,
      z
        .object({
          sha256: z.string().regex(/^[a-f0-9]{64}$/),
          code_commit: z.string().nullable(),
        })
        .strict(),
    ),
    domains: z.array(domainSchema).length(4),
    rpki_days: z.array(day).max(14),
    ris_minutes: z.array(minute).max(60),
    ris_summary: row,
    ris_fixture: z.array(fixture).max(4),
    dns_days: z.array(row).max(2),
    dns_comparison: row,
    representations: z.array(row).max(3),
    queries: z.array(row).max(15),
    processing: z.array(row).max(27),
    hypotheses: z.record(
      z.string(),
      z
        .object({
          decision: z.enum([
            "SUPPORTED_WITHIN_TESTED_SCOPE",
            "NOT_SUPPORTED",
            "INCONCLUSIVE",
            "NOT_RUN",
            "BLOCKED",
          ]),
          sources: z.array(z.string()),
          finding: z.string().optional(),
          limit: z.string().optional(),
          artifact,
        })
        .strict(),
    ),
    negative_findings: z.array(row).max(4),
    operations: z
      .object({
        recorded_date: z.string(),
        evidence: z.literal("RECORDED KUBERNETES RUN"),
        node_count: numeric.int(),
        k3s: z.string(),
        architecture: z.string(),
        vm: z.object({ cpus: numeric, memory_gib: numeric, disk_gib: numeric }),
        peak_sampled_working_set_bytes: numeric,
        measurement: z.string(),
        selected_lag: numeric,
        lag_query_id: z.string(),
        fixture_versions: numeric,
        fixture_diffs: numeric,
        native_pods: z.array(
          z.object({
            name: z.string(),
            uid: z.string(),
            role: z.enum(["driver", "executor"]),
            application_id: z.string(),
            phase: z.string(),
            container_images: z.array(z.string()),
          }),
        ),
        recovery: z.string(),
        limits: z.string(),
        artifact,
        resource_artifact: artifact,
        recovery_artifact: artifact,
        native_artifact: artifact,
      })
      .strict(),
    attribution: z
      .array(z.object({ name: z.string(), scope: z.string(), url: z.url() }))
      .max(4),
  })
  .strict()
  .superRefine((value, ctx) => {
    if (
      value.ris_minutes.reduce((sum, item) => sum + item.occurrences, 0) !==
      value.ris_summary.occurrences
    )
      ctx.addIssue({
        code: "custom",
        message: "RIS accepted minute coverage mismatch",
      });
    if (value.dns_comparison.strict_matched_vantage_eligible !== 0)
      ctx.addIssue({
        code: "custom",
        message: "Unverified matched vantage claim",
      });
    if (value.hypotheses.H3?.decision !== "NOT_RUN")
      ctx.addIssue({
        code: "custom",
        message: "Unexecuted H3 cannot be promoted",
      });
  });
export type Catalog = z.infer<typeof catalogSchema>;
export type Domain = z.infer<typeof domainSchema>;
export type EvidenceRow = {
  id: string;
  label: string;
  unit: string;
  scope: string;
  status: string;
  source: string;
  details: Record<string, unknown>;
  artifact: string | null;
  role?: string;
};
