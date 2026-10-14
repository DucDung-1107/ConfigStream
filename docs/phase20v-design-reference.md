# Phase20V design reference freeze

Only visual foundation: [official dashboard-01](https://ui.shadcn.com/blocks#dashboard-01),
[standalone preview](https://ui.shadcn.com/view/new-york-v4/dashboard-01).
Upstream shadcn-ui/ui commit: `30a58d4498f270af1c1e981a7d78a15cf5740841`.
Source: `apps/v4/registry/new-york-v4/blocks/dashboard-01` and its UI primitives.
All27 inspected registry files were compared byte-for-byte with the pinned source.
Every copied file gets a source SHA-256 in the reference receipt; MIT license is
retained in `frontend/THIRD_PARTY_NOTICES.md`.

Preserve inset/offcanvas SidebarProvider/AppSidebar, 18rem sidebar, 3rem header,
compact neutral cards, reference typography/spacing/radii, responsive sheet,
ChartContainer/Recharts, Tabs/Select and TanStack/shadcn Table relationships.
Keep the composition sidebar → header → section cards → chart → evidence table.

Intentional adaptations: replace all business/demo marketing values; remove
write/admin/account controls, drag sorting and decorative card gradients; no dark
mode in the first accepted profile. Charts use linear measured points, not smooth
invented activity; no sum of incompatible native units. Tables expose only
bounded approved aggregates/owned fixtures or existing server-paginated metadata.
Static Next.js export avoids an additional server-side JavaScript runtime in K3s.
Live configuration/health is same-origin read-only proxy; research aggregates are
prevalidated snapshot exports, not live telemetry or new ingestion.

Reference desktop/mobile screenshots were captured from the actual official
preview, waiting for its SVG clip animation to complete. All seven implemented
screens were inspected against the reference in actual Chrome. See the
[license/source receipt](../workloads/results/phase20v/reference-and-license.json)
and [visual review](phase20v-visual-review.md). Seventeen actual upstream
primitives/hooks and the five named block components are reused/adapted, not
recreated as an unrelated template. Complete MIT notices ship with the static
application, alongside the bundled Geist OFL and Tabler MIT notices.
