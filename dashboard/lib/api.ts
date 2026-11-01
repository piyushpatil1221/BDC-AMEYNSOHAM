"use client";
import { useEffect, useRef, useState } from "react";

export const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
export const GRAFANA = process.env.NEXT_PUBLIC_GRAFANA_URL ?? "http://localhost:3001/d/newsrec";

export async function getJson<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`, { cache: "no-store" });
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
  return r.json() as Promise<T>;
}

/** Poll an endpoint every `ms`; keeps the last good value if a poll fails. */
export function usePoll<T>(path: string | null, ms: number): { data: T | null; error: string | null } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const pathRef = useRef(path);
  pathRef.current = path;
  useEffect(() => {
    if (!path) return;
    let alive = true;
    const tick = async () => {
      try {
        const d = await getJson<T>(path);
        if (alive && pathRef.current === path) {
          setData(d);
          setError(null);
        }
      } catch (e) {
        if (alive) setError(String(e));
      }
    };
    tick();
    const id = setInterval(tick, ms);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [path, ms]);
  return { data, error };
}

export type Stats = {
  latency_ms: { p50: number | null; p95: number | null; p99: number | null; budget: number; requests: number };
  fallback_rate: number;
  paths: Record<string, number>;
  coldstart: { articles: number; within_target: number; coverage: number | null; target_s: number; p50_s: number | null; p95_s: number | null };
  freshness_median_age_h: number | null;
  drift_lags: { burst_id: number; category: string; event_lag_s: number; wall_lag_s: number; old_half_life_h: number; new_half_life_h: number }[];
  drift_events: number;
  event_clock: number;
  recent_cache: number;
  warm_items: number;
};

export type Article = {
  article_id: string; title: string; category: string; subcategory: string; publish_ts: number;
  emitted_at: number; embedded_at: number | null; status: "cold" | "fresh" | "warm"; clicks: number;
  coldstart_delay_s: number | null;
};

export type Rec = {
  user_id: string; path: "ann" | "fallback" | "popular"; latency_ms: number;
  timings: Record<string, number>;
  session: { clicks: number; clicked: string[]; session_end: string | null } | null;
  event_clock: number;
  items: { article_id: string; title: string | null; category: string | null; score: number; similarity: number; warm: number; age_h: number | null; clicks: number }[];
};

export type Topic = {
  category: string; half_life_s: number; ewma_ctr: number; drift_count: number;
  history: { event_time: number; half_life_h: number; ctr: number; drift: boolean }[];
};

export type Drift = {
  events: { category: string; direction: string; event_ts: number; wall_ts: number; old_half_life_s: number; new_half_life_s: number; baseline_ctr: number; current_ctr: number }[];
  bursts: { burst_id: number; category: string; start_event_ts: number }[];
  lags: Stats["drift_lags"];
};

export const fmtEventTime = (ms: number) =>
  new Date(ms).toISOString().replace("T", " ").slice(5, 16) + " UTC";
