import type { ReactNode } from "react";

export function Card({ title, children, right }: { title: string; children: ReactNode; right?: ReactNode }) {
  return (
    <section className="rounded-xl border border-line bg-panel p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <h2 className="text-sm font-semibold">{title}</h2>
        {right}
      </div>
      {children}
    </section>
  );
}

export function Kpi({ label, value, hint, tone }: { label: string; value: string; hint?: string; tone?: "good" | "warn" | "bad" }) {
  const color = tone ? `var(--${tone})` : "var(--ink)";
  return (
    <div className="rounded-xl border border-line bg-panel p-4">
      <div className="text-xs text-muted">{label}</div>
      <div className="mt-1 text-2xl font-semibold tabular-nums" style={{ color }}>
        {value}
      </div>
      {hint && <div className="mt-1 text-xs text-muted">{hint}</div>}
    </div>
  );
}

const BADGE: Record<string, { label: string; color: string; title: string }> = {
  cold: { label: "COLD", color: "var(--bad)", title: "Published, not yet embedded" },
  fresh: { label: "FRESH", color: "var(--series-1)", title: "Embedded and retrievable, no clicks yet" },
  warm: { label: "WARM", color: "var(--good)", title: "Has engagement (clicks)" },
  ann: { label: "ANN", color: "var(--good)", title: "Qdrant answered within budget" },
  fallback: { label: "FALLBACK", color: "var(--warn)", title: "Qdrant too slow: in-memory cache used" },
  popular: { label: "POPULAR", color: "var(--muted)", title: "No session yet: decayed popularity" },
};

export function Badge({ kind }: { kind: string }) {
  const b = BADGE[kind] ?? { label: kind, color: "var(--muted)", title: kind };
  return (
    <span title={b.title} className="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-semibold tracking-wide"
      style={{ borderColor: b.color, color: "var(--ink)" }}>
      <span className="h-1.5 w-1.5 rounded-full" style={{ background: b.color }} />
      {b.label}
    </span>
  );
}

export function ErrorNote({ error }: { error: string | null }) {
  if (!error) return null;
  return <p className="text-xs" style={{ color: "var(--bad)" }}>Serving API unreachable ({error}). Is the stack up?</p>;
}
