import "./globals.css";
import Link from "next/link";
import type { ReactNode } from "react";

export const metadata = { title: "NewsRec Live", description: "Real-time session-based news recommendations on Apache Flink" };

const NAV = [
  { href: "/", label: "Overview" },
  { href: "/user", label: "User recommendations" },
  { href: "/topics", label: "Topic decay & drift" },
];

export default function RootLayout({ children }: { children: ReactNode }) {
  const grafana = process.env.NEXT_PUBLIC_GRAFANA_URL ?? "http://localhost:3001/d/newsrec";
  return (
    <html lang="en">
      <body className="min-h-screen">
        <header className="border-b border-line bg-panel">
          <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-x-6 gap-y-2 px-4 py-3">
            <span className="font-semibold">NewsRec Live</span>
            <nav className="flex flex-wrap gap-4 text-sm">
              {NAV.map((n) => (
                <Link key={n.href} href={n.href} className="text-muted hover:text-ink">{n.label}</Link>
              ))}
              <a href={grafana} target="_blank" rel="noreferrer" className="text-muted hover:text-ink">Grafana ↗</a>
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-6xl px-4 py-6">{children}</main>
      </body>
    </html>
  );
}
