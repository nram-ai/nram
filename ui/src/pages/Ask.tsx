import { Fragment, useState } from "react";
import { Link } from "react-router-dom";
import { useAsk, useMeProjects } from "../hooks/useApi";
import type { AskSource } from "../api/client";

const inputClass =
  "w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring";
const primaryBtn =
  "inline-flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50";

// confidenceLabel buckets the numeric [0,1] confidence into a short word for
// the badge, while the exact value is still shown numerically.
function confidenceLabel(c: number): string {
  if (c >= 0.66) return "high";
  if (c >= 0.33) return "medium";
  return "low";
}

// memoryHref builds the Memory Browser deep-link for a source. The browser
// reads ?project=<projectId>&focus=<memoryId> (see MemoryBrowser's deep-link
// effect), so the project slug is resolved to its id via projectIdBySlug; if
// the project is not in the caller's list we fall back to focus-only.
function memoryHref(s: AskSource, projectIdBySlug: Map<string, string>): string {
  const pid = projectIdBySlug.get(s.project_slug);
  return pid
    ? `/memories?project=${pid}&focus=${s.memory_id}`
    : `/memories?focus=${s.memory_id}`;
}

// renderAnswer splits the answer prose on inline [N] footnote markers and
// renders each as a superscript link to its cited source's memory, so the
// citations read as real footnotes instead of literal text.
function renderAnswer(
  answer: string,
  sources: AskSource[],
  projectIdBySlug: Map<string, string>,
) {
  const byCitation = new Map<number, AskSource>();
  for (const s of sources) {
    if (s.citation) byCitation.set(s.citation, s);
  }
  const parts = answer.split(/(\[\d+\])/g);
  return parts.map((part, i) => {
    const m = /^\[(\d+)\]$/.exec(part);
    if (!m) return <Fragment key={i}>{part}</Fragment>;
    const n = Number(m[1]);
    const src = byCitation.get(n);
    if (!src) return <Fragment key={i}>{part}</Fragment>;
    return (
      <sup key={i} className="mx-0.5">
        <Link
          to={memoryHref(src, projectIdBySlug)}
          className="text-primary no-underline hover:underline"
          title={
            src.score != null
              ? `${src.project_slug} · score ${src.score.toFixed(3)}`
              : `${src.project_slug} · linked via graph/siblings`
          }
        >
          [{n}]
        </Link>
      </sup>
    );
  });
}

export default function Ask() {
  const [query, setQuery] = useState("");
  const [project, setProject] = useState("");
  const ask = useAsk();
  const projectsQuery = useMeProjects();
  const projectIdBySlug = new Map<string, string>(
    (projectsQuery.data ?? []).map((p) => [p.slug, p.id]),
  );

  function submit(e: React.FormEvent) {
    e.preventDefault();
    const q = query.trim();
    if (!q) return;
    ask.mutate({ query: q, project: project || undefined });
  }

  const result = ask.data;

  return (
    <div className="mx-auto max-w-4xl space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Ask</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Ask a question and get one synthesized answer over your stored
          memories, with the sources it drew on. Leave the scope on{" "}
          <span className="font-medium">All projects</span> for a wide synthesis,
          or pick a single project to narrow it. Each ask spends a model call.
        </p>
      </div>

      <form onSubmit={submit} className="space-y-3">
        <div>
          <label className="mb-1 block text-sm font-medium text-muted-foreground">
            Question
          </label>
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            rows={3}
            placeholder="e.g. What did I decide about the recall fusion ranking?"
            className={inputClass}
            onKeyDown={(e) => {
              if ((e.metaKey || e.ctrlKey) && e.key === "Enter") submit(e);
            }}
          />
        </div>

        <div className="flex flex-wrap items-end gap-3">
          <div className="min-w-48">
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Scope
            </label>
            <select
              value={project}
              onChange={(e) => setProject(e.target.value)}
              className={inputClass}
            >
              <option value="">All projects (wide)</option>
              {(projectsQuery.data ?? []).map((p) => (
                <option key={p.id} value={p.slug}>
                  {p.slug}
                </option>
              ))}
            </select>
          </div>
          <button
            type="submit"
            className={primaryBtn}
            disabled={ask.isPending || query.trim() === ""}
          >
            {ask.isPending ? "Asking…" : "Ask"}
          </button>
        </div>
      </form>

      {ask.isError && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {(ask.error as Error)?.message ?? "Ask failed."}
        </div>
      )}

      {result && (
        <div className="space-y-4">
          {result.synthesis_meta.synthesis_failed && (
            <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm">
              The synthesizer did not return an answer. The source memories it
              retrieved are listed below.
            </div>
          )}

          {result.answer && (
            <div className="rounded-md border bg-card p-4">
              <div className="mb-2 flex items-center gap-2">
                <h2 className="text-sm font-semibold">Answer</h2>
                <span className="rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">
                  confidence: {confidenceLabel(result.confidence)} (
                  {result.confidence.toFixed(2)})
                </span>
              </div>
              <p className="whitespace-pre-wrap text-sm leading-relaxed">
                {renderAnswer(result.answer, result.sources, projectIdBySlug)}
              </p>
            </div>
          )}

          <div className="rounded-md border bg-card p-4">
            <h2 className="mb-2 text-sm font-semibold">
              Sources ({result.sources.length})
            </h2>
            {result.sources.length === 0 ? (
              <p className="text-sm text-muted-foreground">No sources.</p>
            ) : (
              <ul className="space-y-1.5">
                {result.sources.map((s) => (
                  <li
                    key={s.memory_id}
                    className="flex items-center justify-between gap-3 text-sm"
                  >
                    <span className="flex min-w-0 items-center gap-2">
                      {s.citation ? (
                        <span className="shrink-0 font-mono text-xs text-muted-foreground">
                          [{s.citation}]
                        </span>
                      ) : null}
                      <Link
                        to={memoryHref(s, projectIdBySlug)}
                        className="truncate font-mono text-xs text-primary hover:underline"
                      >
                        {s.memory_id}
                      </Link>
                    </span>
                    <span className="shrink-0 text-xs text-muted-foreground">
                      {s.project_slug} ·{" "}
                      {s.score != null
                        ? `score ${s.score.toFixed(3)}`
                        : "linked via graph/siblings"}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <p className="text-xs text-muted-foreground">
            {result.synthesis_meta.neighborhood_size} memories considered ·{" "}
            {result.synthesis_meta.latency_ms} ms
          </p>
        </div>
      )}
    </div>
  );
}
