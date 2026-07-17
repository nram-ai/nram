import { useState, type ReactNode } from "react";

// ClearJobsBlock is the shared body of ClearCompletedJobsBlock and
// ClearFailedJobsBlock: a "Keep last N days" input, a confirm strip, and the
// deleted-count / error feedback. Each caller owns its mutation hook and passes
// the copy that differs plus a `run` that maps its hook's argument/response
// shape onto (olderThanDays) -> deleted count. The deletion is irreversible, so
// the primary button opens an explicit confirm rather than firing immediately.
export interface ClearJobsBlockProps {
  title: string;
  description: ReactNode;
  // Primary button label, e.g. "Clear failed jobs".
  buttonLabel: string;
  // Noun for the success line, e.g. "failed" -> "Deleted 3 failed jobs.".
  deletedNoun: string;
  // The confirm-strip question, parameterized by the chosen day window.
  confirmQuestion: (olderThanDays: number) => ReactNode;
  confirmButtonLabel: string;
  isPending: boolean;
  run: (
    olderThanDays: number,
    cb: { onSuccess: (deleted: number) => void; onError: (message: string) => void },
  ) => void;
}

export function ClearJobsBlock(props: ClearJobsBlockProps) {
  const [olderThanDays, setOlderThanDays] = useState<number>(0);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [deleted, setDeleted] = useState<number | null>(null);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  function handleConfirm() {
    setErrMsg(null);
    setConfirmOpen(false);
    setDeleted(null);
    props.run(olderThanDays, {
      onSuccess: (d) => setDeleted(d),
      onError: (msg) => setErrMsg(msg),
    });
  }

  return (
    <div className="mt-4 rounded-md border border-border bg-muted/30 p-4">
      <h4 className="text-sm font-semibold text-foreground">{props.title}</h4>
      <p className="mt-1 text-xs text-muted-foreground">{props.description}</p>
      <div className="mt-3 flex flex-wrap items-center gap-3">
        <label className="flex items-center gap-2 text-xs text-muted-foreground">
          Keep last
          <input
            type="number"
            min={0}
            value={olderThanDays}
            onChange={(e) =>
              setOlderThanDays(Math.max(0, Number(e.target.value) || 0))
            }
            className="w-20 rounded-md border border-input bg-background px-2 py-1 text-sm text-foreground"
          />
          days
        </label>
        <button
          type="button"
          onClick={() => {
            setErrMsg(null);
            setConfirmOpen(true);
          }}
          disabled={props.isPending}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {props.isPending ? "Working…" : props.buttonLabel}
        </button>
        {deleted !== null && (
          <span className="text-xs text-green-700 dark:text-green-400">
            Deleted {deleted} {props.deletedNoun} job{deleted === 1 ? "" : "s"}.
          </span>
        )}
      </div>
      {errMsg && <p className="mt-2 text-xs text-destructive">{errMsg}</p>}

      {confirmOpen && (
        <div className="mt-3 rounded-md border border-yellow-300 bg-yellow-50 p-3 dark:border-yellow-800 dark:bg-yellow-900/30">
          <p className="text-xs text-yellow-900 dark:text-yellow-100">
            {props.confirmQuestion(olderThanDays)}
          </p>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={handleConfirm}
              className="rounded-md bg-destructive px-3 py-1 text-xs font-medium text-destructive-foreground shadow-sm hover:bg-destructive/90"
            >
              {props.confirmButtonLabel}
            </button>
            <button
              type="button"
              onClick={() => setConfirmOpen(false)}
              className="rounded-md border border-input px-3 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted"
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
