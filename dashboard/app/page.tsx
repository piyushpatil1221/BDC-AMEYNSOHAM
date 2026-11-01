"use client";
import { useEffect, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Badge, Card, ErrorNote, Kpi } from "@/components/ui";
import { type Article, type Stats, fmtEventTime, usePoll } from "@/lib/api";

type Point = { t: string; p95: number | null; p99: number | null; fallback: number };

export default function Overview() {
  const { data: stats, error } = usePoll<Stats>("/stats", 3000);
  const { data: feed } = usePoll<Article[]>("/articles/recent?limit=40", 2000);
  const [history, setHistory] = useState<Point[]>([]);

  useEffect(() => {
    if (!stats) return;
    const t = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    setHistory((h) => [...h.slice(-59), { t, p95: stats.latency_ms.p95, p99: stats.latency_ms.p99, fallback: +(stats.fallback_rate * 100).toFixed(1) }]);
  }, [stats]);

  const cov = stats?.coldstart.coverage;
  const p95 = stats?.latency_ms.p95;
  const lag = stats?.drift_lags.at(-1);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Pipeline overview</h1>
        <p className="text-sm text-muted">
          Event clock {stats?.event_clock ? fmtEventTime(stats.event_clock) : "-"} · {stats?.recent_cache ?? 0} recent articles cached ·{" "}
          {stats?.warm_items ?? 0} items with engagement scores
        </p>
        <ErrorNote error={error} />
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <Kpi label="Cold-start coverage" value={cov == null ? "-" : `${(cov * 100).toFixed(1)}%`}
          hint={`retrievable within ${stats?.coldstart.target_s ?? 10} s · p50 ${stats?.coldstart.p50_s ?? "-"} s`}
          tone={cov == null ? undefined : cov >= 0.9 ? "good" : cov >= 0.6 ? "warn" : "bad"} />
        <Kpi label="Recommend p95" value={p95 == null ? "-" : `${p95} ms`} hint={`budget ${stats?.latency_ms.budget ?? 50} ms · p99 ${stats?.latency_ms.p99 ?? "-"} ms`}
          tone={p95 == null ? undefined : p95 <= 50 ? "good" : "bad"} />
        <Kpi label="Fallback rate" value={stats ? `${(stats.fallback_rate * 100).toFixed(1)}%` : "-"} hint={`${stats?.latency_ms.requests ?? 0} requests`} />
        <Kpi label="Freshness (median age)" value={stats?.freshness_median_age_h == null ? "-" : `${stats.freshness_median_age_h} h`} hint="of articles in top-k, event time" />
        <Kpi label="Drift-detection lag" value={lag ? `${lag.event_lag_s.toFixed(0)} s` : "-"}
          hint={lag ? `${lag.category}: half-life ${lag.old_half_life_h.toFixed(1)}h → ${lag.new_half_life_h.toFixed(1)}h (wall ${lag.wall_lag_s.toFixed(0)} s)` : "run `make burst`"} />
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card title="Recommendation latency vs 50 ms budget">
          <div className="h-56">
            <ResponsiveContainer>
              <LineChart data={history} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                <CartesianGrid stroke="var(--line)" vertical={false} />
                <XAxis dataKey="t" tick={{ fontSize: 10, fill: "var(--muted)" }} minTickGap={40} />
                <YAxis unit=" ms" tick={{ fontSize: 10, fill: "var(--muted)" }} width={56} />
                <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--line)", fontSize: 12 }} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <ReferenceLine y={50} stroke="var(--muted)" strokeDasharray="4 4" label={{ value: "budget", fontSize: 10, fill: "var(--muted)", position: "insideTopRight" }} />
                <Line type="monotone" dataKey="p95" name="p95" stroke="var(--series-1)" strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line type="monotone" dataKey="p99" name="p99" stroke="var(--series-2)" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-3 flex flex-wrap gap-3 text-xs text-muted">
            {Object.entries(stats?.paths ?? {}).map(([p, n]) => (
              <span key={p} className="flex items-center gap-1"><Badge kind={p} /> {n}</span>
            ))}
          </div>
        </Card>

        <Card title="Live article feed (cold-start status)">
          <ul className="max-h-72 divide-y divide-line overflow-y-auto pr-1 text-sm">
            {(feed ?? []).map((a) => (
              <li key={a.article_id} className="flex items-start gap-2 py-2">
                <Badge kind={a.status} />
                <div className="min-w-0 flex-1">
                  <div className="truncate" title={a.title}>{a.title}</div>
                  <div className="text-xs text-muted">
                    {a.article_id} · {a.category} · published {fmtEventTime(a.publish_ts)}
                    {a.coldstart_delay_s != null && <> · retrievable in {a.coldstart_delay_s}s</>}
                    {a.clicks > 0 && <> · {a.clicks} clicks</>}
                  </div>
                </div>
              </li>
            ))}
            {!feed?.length && <li className="py-2 text-muted">No articles yet: run the replay.</li>}
          </ul>
        </Card>
      </div>
    </div>
  );
}
