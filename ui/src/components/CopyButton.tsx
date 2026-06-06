import { useCallback, useEffect, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCopy, faCheck } from "../lib/icons";
import { copyToClipboard } from "../lib/clipboard";

interface CopyButtonProps {
  // Text placed on the clipboard when clicked.
  text: string;
  // Resting label. Defaults to "Copy".
  label?: string;
  // Label shown briefly after a successful copy. Defaults to "Copied".
  copiedLabel?: string;
  // Tailwind classes for the button. Each call site passes its own so the
  // existing per-page styling is preserved.
  className?: string;
  // Optional native title/tooltip.
  title?: string;
  // Render the faCopy/faCheck icon ahead of the label (used by the wizard).
  withIcon?: boolean;
}

// How long the copied state stays visible after a successful copy, in ms.
const COPIED_RESET_MS = 2000;

const DEFAULT_CLASSNAME =
  "rounded border border-input bg-background px-1.5 py-0.5 text-xs text-muted-foreground hover:bg-muted";

// CopyButton is the single shared copy affordance for the UI. It routes the
// copy through copyToClipboard (which works in both secure and insecure
// contexts) and only flips to the copied state when the write actually
// succeeds.
export function CopyButton({
  text,
  label = "Copy",
  copiedLabel = "Copied",
  className,
  title,
  withIcon = false,
}: CopyButtonProps) {
  const [copied, setCopied] = useState(false);
  const resetTimer = useRef<number | undefined>(undefined);

  useEffect(() => () => window.clearTimeout(resetTimer.current), []);

  const handleCopy = useCallback(async () => {
    const ok = await copyToClipboard(text);
    if (!ok) return;
    setCopied(true);
    window.clearTimeout(resetTimer.current);
    resetTimer.current = window.setTimeout(() => setCopied(false), COPIED_RESET_MS);
  }, [text]);

  return (
    <button
      type="button"
      onClick={handleCopy}
      className={className ?? DEFAULT_CLASSNAME}
      title={title}
    >
      {withIcon ? (
        <>
          <FontAwesomeIcon
            icon={copied ? faCheck : faCopy}
            className="h-3.5 w-3.5"
          />
          {copied ? copiedLabel : label}
        </>
      ) : copied ? (
        copiedLabel
      ) : (
        label
      )}
    </button>
  );
}
