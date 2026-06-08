import { Logo } from "./Logo";

// Centered brand lockup for the unauthenticated entry pages (login, OAuth
// authorize): the hero tile above the product tagline. Sits inside each page's
// `text-center` block, above the page-specific heading. The wordmark is hidden
// because the headings already carry the "Neural Ram" name.
export function AuthBrand() {
  return (
    <>
      <div className="flex justify-center">
        <Logo size="xl" showWordmark={false} />
      </div>
      <p className="mt-3 text-sm text-muted-foreground">
        The continuity layer for everything you do with AI.
      </p>
    </>
  );
}
