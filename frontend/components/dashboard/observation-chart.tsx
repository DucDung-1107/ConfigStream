"use client";

import { Line, LineChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";

export function ObservationChart({
  title,
  description,
  points,
  unit,
}: {
  title: string;
  description: string;
  points: { date: string; value: number }[];
  unit: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer
          config={{ value: { label: unit, color: "var(--chart-1)" } }}
          className="h-[250px] w-full"
          aria-label={`${title}, ${unit}`}
          role="figure"
        >
          <LineChart
            data={points}
            accessibilityLayer
            margin={{ left: 16, right: 16 }}
          >
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="date"
              tickLine={false}
              axisLine={false}
              minTickGap={20}
            />
            <YAxis
              width={60}
              tickLine={false}
              axisLine={false}
              domain={[0, "auto"]}
            />
            <ChartTooltip content={<ChartTooltipContent />} />
            <Line
              type="linear"
              dataKey="value"
              stroke="var(--chart-1)"
              strokeWidth={2}
              dot={{ r: 3 }}
              connectNulls={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ChartContainer>
        <p className="mt-3 text-xs text-muted-foreground">
          Accepted real-source aggregate · {unit} within the declared selected
          scope.
        </p>
      </CardContent>
    </Card>
  );
}
