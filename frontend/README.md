# ConfigStream research dashboard

Actual shadcn/ui dashboard-01 foundation, static Next.js App Router, read-only
approved aggregate evidence and same-origin configuration metadata. See
`../docs/phase20v-design-reference.md` and `THIRD_PARTY_NOTICES.md`.

```sh
npm ci
npm run typecheck
npm test
npm run build
npm run e2e
```

E2E uses owned API fixtures; `tests/review.mjs` captures a real backend demo.
Local build: `node tests/serve.mjs`, with the existing API forwarded to127.0.0.1:18800.
Kubernetes deployment and localhost13080 access are in the phase20v runbook.
The catalog is regenerated with `.venv/bin/python -m dashboard.export` from the
repository root; all pinned source hashes must match. Never copy private data
into `public/`. Build and snapshot SHA must move together or read fails closed.
