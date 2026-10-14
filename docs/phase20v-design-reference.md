# Phase20V design reference freeze

Only visual foundation: [official dashboard-01](https://ui.shadcn.com/blocks#dashboard-01),
[standalone preview](https://ui.shadcn.com/view/new-york-v4/dashboard-01).
Upstream shadcn-ui/ui commit: `30a58d4498f270af1c1e981a7d78a15cf5740841`.
Source: `apps/v4/registry/new-york-v4/blocks/dashboard-01` and its UI primitives.
The served registry sidebar was compared byte-for-byte with the pinned source.
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

Reference screenshots and final visual review remain acceptance gates until
actual browser capture and inspection are recorded. No second template is used.
