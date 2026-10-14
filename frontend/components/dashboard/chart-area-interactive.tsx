"use client";

import { useState } from "react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardAction,
  CardContent,
} from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
import { count } from "@/lib/formatting";
import type { Catalog } from "@/lib/schemas/catalog";

const config = {
  announcement: { label: "Announcements", color: "var(--chart-1)" },
  withdrawal: { label: "Withdrawals", color: "var(--chart-2)" },
};
export function ChartAreaInteractive({
  minutes,
}: {
  minutes: Catalog["ris_minutes"];
}) {
  const [range, setRange] = useState("60");
  const selected = minutes.slice(0, Number(range));
  return (
    <Card className="@container/card">
      <CardHeader className="gap-3 has-data-[slot=card-action]:grid-cols-1 sm:has-data-[slot=card-action]:grid-cols-[1fr_auto]">
        <CardTitle>RIS source-time activity</CardTitle>
        <CardDescription>
          rrc00 · September 1, 2026 · native occurrences per minute (UTC)
        </CardDescription>
        <CardAction className="col-start-1 row-start-auto sm:col-start-2 sm:row-start-1">
          <Select value={range} onValueChange={setRange}>
            <SelectTrigger aria-label="RIS time window" className="w-40">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="60">Full source hour</SelectItem>
              <SelectItem value="15">First 15 minutes</SelectItem>
              <SelectItem value="5">First 5 minutes</SelectItem>
            </SelectContent>
          </Select>
        </CardAction>
      </CardHeader>
      <CardContent className="px-2 pt-4 sm:px-6">
        <ChartContainer
          config={config}
          className="h-[250px] w-full"
          aria-label="Accepted routing occurrences by source minute"
          role="figure"
        >
          <AreaChart
            data={selected}
            accessibilityLayer
            margin={{ left: 8, right: 8 }}
          >
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="time"
              tickLine={false}
              axisLine={false}
              minTickGap={28}
              tickFormatter={(value) => String(value).slice(11, 16)}
            />
            <YAxis
              tickLine={false}
              axisLine={false}
              width={45}
              tickFormatter={(value) => `${Number(value) / 1000}k`}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  labelFormatter={(value) =>
                    `${String(value).slice(11, 16)} UTC`
                  }
                  formatter={(value, name) => (
                    <>
                      <span>{name}</span>
                      <span className="ml-auto font-mono">
                        {count(Number(value))} occurrences
                      </span>
                    </>
                  )}
                />
              }
            />
            <Area
              type="linear"
              dataKey="withdrawal"
              stackId="events"
              stroke="var(--chart-2)"
              fill="var(--chart-2)"
              fillOpacity={0.15}
              isAnimationActive={false}
            />
            <Area
              type="linear"
              dataKey="announcement"
              stackId="events"
              stroke="var(--chart-1)"
              fill="var(--chart-1)"
              fillOpacity={0.1}
              isAnimationActive={false}
            />
          </AreaChart>
        </ChartContainer>
        <p className="mt-4 text-xs text-muted-foreground">
          Accepted real-source aggregate · historical source time; excludes
          parser errors. Kafka/Spark processing throughput is a separate
          measurement.
        </p>
      </CardContent>
    </Card>
  );
}
