import { afterEach, describe, expect, it, vi } from "vitest";
import { getMetadata } from "@/lib/api/client";

afterEach(() => vi.unstubAllGlobals());

describe("live metadata requests", () => {
  it.each([
    ["2026-10-31", "2026-11-01T00:00:00.000Z"],
    ["2028-02-29", "2028-03-01T00:00:00.000Z"],
    ["2026-12-31", "2027-01-01T00:00:00.000Z"],
  ])(
    "includes all of %s without including the next day",
    async (end, nextDay) => {
      const fetcher = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            items: [],
            count: 0,
            has_more: false,
            next_cursor: null,
          }),
        ),
      );
      vi.stubGlobal("fetch", fetcher);
      await getMetadata("snapshots", "router/a", null, end, end);
      const url = new URL(fetcher.mock.calls[0][0], "http://localhost");
      expect(url.searchParams.get("end_before")).toBe(nextDay);
      expect(url.searchParams.has("end")).toBe(false);
      expect(url.searchParams.get("asset_id")).toBe("router/a");
    },
  );

  it("rejects impossible calendar dates before making a request", async () => {
    const fetcher = vi.fn();
    vi.stubGlobal("fetch", fetcher);
    await expect(
      getMetadata("snapshots", "router", null, "", "2026-02-29"),
    ).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("cancels obsolete requests when filters change", async () => {
    const controller = new AbortController();
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          items: [],
          count: 0,
          has_more: false,
          next_cursor: null,
        }),
      ),
    );
    vi.stubGlobal("fetch", fetcher);
    await getMetadata("snapshots", "router", null, "", "", controller.signal);
    const signal: AbortSignal = fetcher.mock.calls[0][1].signal;
    expect(signal.aborted).toBe(false);
    controller.abort();
    expect(signal.aborted).toBe(true);
  });
});
