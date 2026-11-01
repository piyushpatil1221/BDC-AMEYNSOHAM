"use client";
import { useEffect, useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ReferenceDot, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Card, ErrorNote } from "@/components/ui";
import { type Drift, type Topic, fmtEventTime, usePoll } from "@/lib/api";

export default function TopicsPage() {
  const { data: topics, error } = usePoll<Topic[]>("/topics?history=300", 5000);
  const { data: drift } = usePoll<Drift>("/drift", 5000);
  const [category, setCategory] = useState("");

  useEffect(() => {
    if (category || !topics?.length) return;
    const burst = drift?.bursts.at(-1)?.category;
    setCategory(burst && topics.some((t) => t.category === burst) ? burst : topics[0].category);
  }, [topics, drift, category]);

  const topic = topics?.find((t) => t.category === category);
  const data = useMemo(() => (topic?.history ?? []).map((h) => ({ ...h, label: fmtEventTime(h.event_time) })), [topic]);
  const drifts = data.filter((d) => d.drift);
  const events = (drift?.events ?? []).filter((e) => e.category === category);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Topic half-life and drift</h1>
        <p className="text-sm text-muted">
          Each topic&apos;s relevance decays as exp(−ln2 · age / half-life). River ADWIN watches engagement; a spike shortens the half-life,
          a slow burn lengthens it, and between drifts it relaxes back toward 6 h.
        </p>
        <ErrorNote error={error} />
      </div>

      <div className="flex flex-wrap gap-2">
        {(topics ?? []).map((t) => (
          <button key={t.category} onClick={() => setCategory(t.category)}
            className={`rounded-full border px-3 py-1 text-xs ${t.category === category ? "border-accent font-semibold" : "border-line text-muted"}`}>
            {t.category} · {(t.half_life_s / 3600).toFixed(1)}h{t.drift_count ? ` · ${t.drift_count} drift` : ""}
          </button>
        ))}
      </div>

      <Card title={`Half-life of “${category}” (roll-ups every 10 event-minutes; dots mark drift)`}>
        <div className="h-72">
          <ResponsiveContainer>
            <LineChart data={data} margin={{ top: 12, right: 20, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="var(--line)" vertical={false} />
              <XAxis dataKey="label" tick={{ fontSize: 10, fill: "var(--muted)" }} minTickGap={60} />
              <YAxis unit=" h" domain={[0, "auto"]} tick={{ fontSize: 10, fill: "var(--muted)" }} width={50} />
              <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--line)", fontSize: 12 }}
                formatter={(v: number, n: string) => (n === "half_life_h" ? [`${v.toFixed(2)} h`, "half-life"] : [v.toFixed(3), "CTR"])} />
              <Line type="stepAfter" dataKey="half_life_h" stroke="var(--series-1)" strokeWidth={2} dot={false} isAnimationActive={false} />
              {drifts.map((d) => (
                <ReferenceDot key={d.event_time} x={d.label} y={d.half_life_h} r={6} fill="var(--series-2)" stroke="var(--panel)" strokeWidth={2} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Card>

      <Card title="Drift events for this topic">
        {events.length ? (
          <table className="w-full text-left text-sm">
            <thead className="text-xs text-muted"><tr><th className="py-1">Event time</th><th>Direction</th><th>Half-life</th><th>CTR (before → after)</th></tr></thead>
            <tbody className="divide-y divide-line">
              {events.map((e, i) => (
                <tr key={i}>
                  <td className="py-1.5">{fmtEventTime(e.event_ts)}</td>
                  <td>{e.direction}</td>
                  <td className="tabular-nums">{(e.old_half_life_s / 3600).toFixed(1)}h → {(e.new_half_life_s / 3600).toFixed(1)}h</td>
                  <td className="tabular-nums">{e.baseline_ctr.toFixed(3)} → {e.current_ctr.toFixed(3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="text-sm text-muted">No drift detected for this topic yet. Inject one with <code>make burst ARGS=&quot;--category {category || "sports"}&quot;</code>.</p>
        )}
      </Card>
    </div>
  );
}
