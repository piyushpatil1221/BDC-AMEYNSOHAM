"use client";
import { useEffect, useState } from "react";
import { Badge, Card, ErrorNote } from "@/components/ui";
import { type Rec, usePoll } from "@/lib/api";

type ActiveUser = { user_id: string; clicks: number; session_end: string | null };

export default function UserPage() {
  const { data: users, error } = usePoll<ActiveUser[]>("/users/active?limit=30", 5000);
  const [userId, setUserId] = useState<string>("");
  const [manual, setManual] = useState("");
  const { data: rec } = usePoll<Rec>(userId ? `/recommend/${encodeURIComponent(userId)}?k=10` : null, 2000);

  useEffect(() => {
    if (!userId && users?.length) setUserId(users[0].user_id);
  }, [users, userId]);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Live recommendations for one user</h1>
        <p className="text-sm text-muted">Refreshes every 2 s. As the user clicks during the replay, Flink updates their session vector and the list re-ranks.</p>
        <ErrorNote error={error} />
      </div>

      <div className="flex flex-wrap items-end gap-3">
        <label className="text-sm">
          <span className="mb-1 block text-xs text-muted">Active sessions (most recent first)</span>
          <select className="rounded-md border border-line bg-panel px-2 py-1.5 text-sm" value={userId} onChange={(e) => setUserId(e.target.value)}>
            {(users ?? []).map((u) => (
              <option key={u.user_id} value={u.user_id}>{u.user_id} · {u.clicks} clicks</option>
            ))}
          </select>
        </label>
        <form className="flex items-end gap-2" onSubmit={(e) => { e.preventDefault(); if (manual.trim()) setUserId(manual.trim()); }}>
          <label className="text-sm">
            <span className="mb-1 block text-xs text-muted">Or any user id</span>
            <input className="w-36 rounded-md border border-line bg-panel px-2 py-1.5 text-sm" placeholder="U12345" value={manual} onChange={(e) => setManual(e.target.value)} />
          </label>
          <button className="rounded-md border border-line bg-panel px-3 py-1.5 text-sm hover:border-accent" type="submit">Show</button>
        </form>
      </div>

      {rec && (
        <div className="grid gap-6 lg:grid-cols-3">
          <Card title="Request">
            <dl className="space-y-2 text-sm">
              <div className="flex justify-between"><dt className="text-muted">Path</dt><dd><Badge kind={rec.path} /></dd></div>
              <div className="flex justify-between"><dt className="text-muted">Latency</dt><dd className="tabular-nums">{rec.latency_ms} ms</dd></div>
              {Object.entries(rec.timings).map(([k, v]) => (
                <div key={k} className="flex justify-between text-xs"><dt className="text-muted">{k.replace("_ms", "")}</dt><dd className="tabular-nums">{v} ms</dd></div>
              ))}
              <div className="flex justify-between"><dt className="text-muted">Session clicks</dt><dd>{rec.session?.clicks ?? "no session"}</dd></div>
              <div><dt className="text-muted">Recently clicked (excluded)</dt><dd className="mt-1 break-words text-xs">{rec.session?.clicked.join(", ") || "-"}</dd></div>
            </dl>
          </Card>
          <Card title="Top 10 (blend: 0.6 × similarity + 0.4 × decayed engagement)">
            <ol className="-mt-1 divide-y divide-line text-sm lg:col-span-2">
              {rec.items.map((it, i) => (
                <li key={it.article_id} className="flex gap-3 py-2">
                  <span className="w-5 text-right text-muted tabular-nums">{i + 1}</span>
                  <div className="min-w-0 flex-1">
                    <div className="truncate" title={it.title ?? ""}>{it.title ?? it.article_id}</div>
                    <div className="text-xs text-muted">
                      {it.category} · score {it.score.toFixed(3)} (sim {it.similarity.toFixed(2)}, warm {it.warm.toFixed(2)})
                      {it.age_h != null && <> · {it.age_h} h old</>}
                    </div>
                  </div>
                </li>
              ))}
            </ol>
          </Card>
        </div>
      )}
    </div>
  );
}
