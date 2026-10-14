import {
  Card,
  CardHeader,
  CardDescription,
  CardTitle,
  CardFooter,
} from "@/components/ui/card";
import { count } from "@/lib/formatting";

export type MetricCard = {
  label: string;
  value: number | null;
  unit: string;
  scope: string;
};
export function SectionCards({ cards }: { cards: MetricCard[] }) {
  return (
    <div className="grid grid-cols-1 gap-4 px-4 lg:px-6 @xl/main:grid-cols-2 @5xl/main:grid-cols-4">
      {cards.map((card) => (
        <Card key={card.label} className="@container/card shadow-xs">
          <CardHeader>
            <CardDescription>{card.label}</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
              {card.value === null ? "Not measured" : count(card.value)}
            </CardTitle>
            <div className="text-sm">{card.unit}</div>
          </CardHeader>
          <CardFooter className="flex-col items-start gap-1.5 text-xs text-muted-foreground">
            {card.scope}
          </CardFooter>
        </Card>
      ))}
    </div>
  );
}
