"use client";

import { useState } from "react";
import {
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import {
  IconChevronLeft,
  IconChevronRight,
  IconCopy,
  IconArrowsSort,
} from "@tabler/icons-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet";
import { sourceUrl } from "@/lib/formatting";
import type { EvidenceRow } from "@/lib/schemas/catalog";

export function DataTable({
  data,
  title,
  commit,
}: {
  data: EvidenceRow[];
  title: string;
  commit: string;
}) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("all");
  const [detail, setDetail] = useState<EvidenceRow | null>(null);
  const [copied, setCopied] = useState(false);
  const columns: ColumnDef<EvidenceRow>[] = [
    {
      accessorKey: "label",
      header: ({ column }) => (
        <Button
          variant="ghost"
          size="sm"
          onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
        >
          Record <IconArrowsSort />
        </Button>
      ),
      cell: ({ row }) => (
        <Button
          variant="link"
          className="h-auto max-w-64 whitespace-normal p-0 text-left"
          onClick={() => {
            setCopied(false);
            setDetail(row.original);
          }}
        >
          {row.original.label}
        </Button>
      ),
    },
    { accessorKey: "unit", header: "Native unit / value" },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ getValue }) => (
        <Badge variant="outline" className="font-normal">
          {String(getValue())}
        </Badge>
      ),
    },
    { accessorKey: "scope", header: "Selected scope" },
    {
      accessorKey: "source",
      header: "Evidence / semantic role",
      cell: ({ row }) => (
        <div className="space-y-1">
          {row.original.role && <div>{row.original.role}</div>}
          <div className="text-xs text-muted-foreground">
            {row.original.source}
          </div>
        </div>
      ),
    },
  ];
  const table = useReactTable({
    data: status === "all" ? data : data.filter((row) => row.status === status),
    columns,
    state: { sorting, globalFilter: search },
    onSortingChange: setSorting,
    onGlobalFilterChange: setSearch,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: 10 } },
  });
  const statuses = Array.from(new Set(data.map((row) => row.status)));
  return (
    <section className="flex flex-col gap-4" aria-label={title}>
      <div className="flex flex-wrap items-end gap-3">
        <div className="mr-auto">
          <h2 className="text-base font-semibold">{title}</h2>
          <p className="text-xs text-muted-foreground">
            {data.length} approved records · filters cover the complete bounded
            export
          </p>
        </div>
        <div>
          <Label htmlFor={`search-${title}`} className="mb-1 text-xs">
            Search records
          </Label>
          <Input
            id={`search-${title}`}
            className="w-48"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Filter approved records"
          />
        </div>
        <Select value={status} onValueChange={setStatus}>
          <SelectTrigger
            aria-label={`Status filter for ${title}`}
            className="w-40"
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All statuses</SelectItem>
            {statuses.map((item) => (
              <SelectItem key={item} value={item}>
                {item}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="overflow-x-auto rounded-lg border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((group) => (
              <TableRow key={group.id}>
                {group.headers.map((header) => (
                  <TableHead key={header.id}>
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext(),
                    )}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell
                      key={cell.id}
                      className="max-w-72 whitespace-normal align-top"
                    >
                      {flexRender(
                        cell.column.columnDef.cell,
                        cell.getContext(),
                      )}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={columns.length}
                  className="h-24 text-center"
                >
                  No matching evidence. Missing observations do not establish
                  empty state.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <p className="text-xs text-muted-foreground sm:hidden">
        Scroll the table horizontally for scope and evidence columns.
      </p>
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>
          {table.getFilteredRowModel().rows.length} records · Page{" "}
          {table.getState().pagination.pageIndex + 1} of{" "}
          {Math.max(1, table.getPageCount())}
        </span>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="icon-sm"
            aria-label={`Previous page of ${title}`}
            disabled={!table.getCanPreviousPage()}
            onClick={() => table.previousPage()}
          >
            <IconChevronLeft />
          </Button>
          <Button
            variant="outline"
            size="icon-sm"
            aria-label={`Next page of ${title}`}
            disabled={!table.getCanNextPage()}
            onClick={() => table.nextPage()}
          >
            <IconChevronRight />
          </Button>
        </div>
      </div>
      <Sheet
        open={detail !== null}
        onOpenChange={(open) => {
          if (!open) setDetail(null);
        }}
      >
        <SheetContent className="overflow-y-auto">
          <SheetHeader>
            <SheetTitle>{detail?.label}</SheetTitle>
            <SheetDescription>
              {detail?.source} · {detail?.scope}
            </SheetDescription>
          </SheetHeader>
          {detail && (
            <div className="space-y-4 px-4 pb-6 text-sm">
              <div className="break-all font-mono text-xs">{detail.id}</div>
              <Button
                size="sm"
                variant="outline"
                onClick={async () => {
                  try {
                    await navigator.clipboard.writeText(detail.id);
                    setCopied(true);
                  } catch {
                    setCopied(false);
                  }
                }}
              >
                <IconCopy />
                {copied ? "Copied" : "Copy safe identifier"}
              </Button>
              <span className="sr-only" aria-live="polite">
                {copied ? "Identifier copied" : ""}
              </span>
              <dl className="space-y-3">
                {Object.entries(detail.details).map(([key, value]) => (
                  <div key={key}>
                    <dt className="text-xs text-muted-foreground">
                      {key.replaceAll("_", " ")}
                    </dt>
                    <dd className="break-words">
                      {typeof value === "object"
                        ? JSON.stringify(value)
                        : String(value ?? "Not available")}
                    </dd>
                  </div>
                ))}
              </dl>
              <a
                className="underline"
                href={sourceUrl(commit, detail.artifact)}
                target="_blank"
                rel="noopener noreferrer"
              >
                Open source evidence
              </a>
            </div>
          )}
        </SheetContent>
      </Sheet>
    </section>
  );
}
