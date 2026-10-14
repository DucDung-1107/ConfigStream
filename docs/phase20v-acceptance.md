# Phase20V acceptance

**PASS — REFERENCE-DRIVEN DASHBOARD AND KUBERNETES DEMO VALIDATED.**

Local data, functional, visual, deployment and rehearsal gates passed. Release
also requires successful CI on the exact final pushed delivery SHA; its immutable
run SHA/URL/conclusion is recorded in the final handoff, not self-referentially
embedded in the contents of its own Git commit.

Entry: clean main `9c2520c06aa3b455577ead9ef16f0a820e1e804d`,
[baseline CI](https://github.com/kongwoang/ConfigStream/actions/runs/38521108108).
Final runtime source `cceeb827dc59771d324ed5615a699e966b2d051b`; later delivery changes
are measurement scripts, manifests, reviewed screenshots and reports, not altered
business logic. The actual runtime image is pinned in the frontend manifest.

## Verified gates

- Actual dashboard-01 at upstream `30a58d4498f270af1c1e981a7d78a15cf5740841`:
  27 registry files matched pinned source,17 primitives/hooks reused. MIT/OFL
  font/icon notices retained and shipped; no second UI template.
- Next16.4.0/React19.3.0/TypeScript5.9.3/Tailwind4.3.3 static export with exact
  dependency lock. Seven screens preserve the reference shell/components.
- Seventeen immutable receipt hashes bind a52866-byte public aggregate bundle.
  Pydantic plus strict Zod/SHA256/max200KB fail closed. No private ledgers,
  source mounts, configuration bodies or browser credentials.
- Four units remain distinct:10188 Git versions,14 complete RPKI days,4725156 RIS
  occurrences,2234863 DNS measurements. DNS uncertainty/zero strict eligibility,
  RIS multiplicity and H3 NOT_RUN stay explicit. Owned details are labeled.
- Fourteen screenshots inspected against actual reference desktop/mobile;
  1440×900,768×1024,390×844 have no page overflow, bounded table scrolling and
  usable navigation. Seven selected axe scans: zero reported violations, not
  formal accessibility certification.
- Nineteen browser tests passed on the deployed frontend with routed API/fault
  fixtures. Separately the unmocked demo verified two actual configuration
  versions/one adjacent diff, source controls, all routes and fresh API READY.
  Zero unexpected page/console errors; deliberate503 reported separately.
- K3s v1.35.9+k3s1 one ARM64 node; dedicated VM3CPU/6GiB/48GiB. Frontend
  Ready1/1/restart0, nonroot/readonly root/no SA token, ClusterIP only. Observed
  1mCPU/2MiB working set and25.2MiB image, not true peak RSS or HA proof.
- Local regression1128 unit/quality,80 API,13 frontend data tests,19 browser
  tests, none skipped. Prior dashboard CI passed109 Spark tests; final pushed
  CI must recheck them. Terraform/RPKI/RIS/DNS production contracts and previous
  scientific receipts remain unchanged. UI rehearsal is not a new real-source
  recovery trial.

## Evidence and limitations

[Machine-readable acceptance](../workloads/results/phase20v/acceptance.json),
[source binding](../workloads/results/phase20v/evidence-binding.json),
[regression](../workloads/results/phase20v/regression-validation.json),
[deployment](../workloads/results/phase20v/kubernetes-deployment.json),
[resources/negative attempts](../workloads/results/phase20v/resource-measurements.json).

[Additive final delivery](../workloads/results/phase20v/final-delivery-validation.json)
records the final runtime and current14 screenshot checksums,19 tests in14.0s,
unmocked API demo and new12.290-second guarded packaging attempt. Earlier runtime
receipts remain intact. The final configuration table disables local page-search;
asset/date filters search on the server and page-local sorting is explicitly labeled.

The initial full-container build stopped on host swap guard. Runtime Dockerignore
failure, port-forward race, premature screenshots and favicon404 were corrected,
not erased. The accepted lightweight build uses host Node26.5.0; the full Node24
Dockerfile remains available and Linux CI uses Node24. Historical20K.1 resource
samples still bind the original7GiB VM and are labeled RECORDED.

Full Phase20 remains **CONDITIONAL**, RIS paced H3 **NOT_RUN**. Instructor approval
is unverified; source rights/attribution restrictions persist. This closes the
UI teaching-demo gate, not final instructor/course acceptance, public security,
multi-node availability or new full-source benchmarks.

See [reference](phase20v-design-reference.md), [architecture](phase20v-information-architecture.md),
[contracts](phase20v-data-contracts.md), [visual review](phase20v-visual-review.md),
[deployment](phase20v-kubernetes-deployment.md), [demo](phase20v-demo-script.md).

Tóm tắt: dashboard bám đúng shadcn, chạy thật trên K3s và truy vấn API thật;
dữ liệu tổng hợp và fixture được tách rõ, không biến DNS thiếu quan sát thành
xóa cấu hình. Phase20 đầy đủ và phê duyệt giảng viên vẫn là gate riêng.
Không bắt đầu Phase21 hoặc benchmark/ingestion nguồn đầy đủ mới.
