package admin

import (
	"context"
	"testing"

	"github.com/nram-ai/nram/internal/storage"
)

func newTestSetupStore(db storage.DB) *SetupStore {
	return NewSetupStore(
		storage.NewUserRepo(db),
		storage.NewNamespaceRepo(db),
		storage.NewOrganizationRepo(db),
		storage.NewAPIKeyRepo(db),
		storage.NewProjectRepo(db),
		db,
	)
}

// TestIsOnboardingComplete covers the truth table: an explicit "true"/"false"
// flag is honored verbatim, while an unset flag falls back to setup completion
// so legacy installs (set up before the onboarding wizard existed) are treated
// as already onboarded and never see the wizard.
func TestIsOnboardingComplete(t *testing.T) {
	for _, be := range adminTestBackends {
		t.Run(be.name, func(t *testing.T) {
			db := be.setup(t)
			ctx := context.Background()
			store := newTestSetupStore(db)

			// Unset flag, setup not complete -> false.
			got, err := store.IsOnboardingComplete(ctx)
			if err != nil {
				t.Fatalf("IsOnboardingComplete (unset, no setup): %v", err)
			}
			if got {
				t.Fatalf("unset flag with no setup: want false, got true")
			}

			// Unset flag, but setup IS complete (legacy install) -> true.
			if err := storage.SetSystemMeta(ctx, db, "setup_complete", "true"); err != nil {
				t.Fatalf("seed setup_complete: %v", err)
			}
			got, err = store.IsOnboardingComplete(ctx)
			if err != nil {
				t.Fatalf("IsOnboardingComplete (unset, legacy): %v", err)
			}
			if !got {
				t.Fatalf("unset flag with setup complete (legacy): want true, got false")
			}

			// Explicit "false" overrides the legacy fallback -> false.
			if err := storage.SetSystemMeta(ctx, db, "onboarding_complete", "false"); err != nil {
				t.Fatalf("seed onboarding_complete=false: %v", err)
			}
			got, err = store.IsOnboardingComplete(ctx)
			if err != nil {
				t.Fatalf("IsOnboardingComplete (explicit false): %v", err)
			}
			if got {
				t.Fatalf("explicit false: want false, got true")
			}

			// Explicit "true" -> true.
			if err := storage.SetSystemMeta(ctx, db, "onboarding_complete", "true"); err != nil {
				t.Fatalf("seed onboarding_complete=true: %v", err)
			}
			got, err = store.IsOnboardingComplete(ctx)
			if err != nil {
				t.Fatalf("IsOnboardingComplete (explicit true): %v", err)
			}
			if !got {
				t.Fatalf("explicit true: want true, got false")
			}
		})
	}
}

// TestCompleteSetupSeedsOnboardingFalse verifies that creating the first admin
// account explicitly marks onboarding as NOT complete, so a brand-new install
// enters the guided wizard.
func TestCompleteSetupSeedsOnboardingFalse(t *testing.T) {
	for _, be := range adminTestBackends {
		t.Run(be.name, func(t *testing.T) {
			db := be.setup(t)
			ctx := context.Background()
			store := newTestSetupStore(db)

			if _, _, err := store.CompleteSetup(ctx, "admin@example.com", "password123"); err != nil {
				t.Fatalf("CompleteSetup: %v", err)
			}

			val, err := storage.GetSystemMeta(ctx, db, "onboarding_complete")
			if err != nil {
				t.Fatalf("read onboarding_complete: %v", err)
			}
			if val != "false" {
				t.Fatalf("after CompleteSetup: want onboarding_complete=\"false\", got %q", val)
			}

			done, err := store.IsOnboardingComplete(ctx)
			if err != nil {
				t.Fatalf("IsOnboardingComplete: %v", err)
			}
			if done {
				t.Fatalf("after CompleteSetup: want onboarding incomplete, got complete")
			}
		})
	}
}

// TestSetOnboardingProgress verifies the step cursor persists and that marking
// complete flips IsOnboardingComplete to true.
func TestSetOnboardingProgress(t *testing.T) {
	for _, be := range adminTestBackends {
		t.Run(be.name, func(t *testing.T) {
			db := be.setup(t)
			ctx := context.Background()
			store := newTestSetupStore(db)

			// Advance the cursor without completing.
			if err := store.SetOnboardingProgress(ctx, "fact", false); err != nil {
				t.Fatalf("SetOnboardingProgress(step): %v", err)
			}
			step, err := store.OnboardingStep(ctx)
			if err != nil {
				t.Fatalf("OnboardingStep: %v", err)
			}
			if step != "fact" {
				t.Fatalf("step cursor: want \"fact\", got %q", step)
			}
			done, err := store.IsOnboardingComplete(ctx)
			if err != nil {
				t.Fatalf("IsOnboardingComplete: %v", err)
			}
			if done {
				t.Fatalf("after step-only progress: want incomplete, got complete")
			}

			// Mark complete.
			if err := store.SetOnboardingProgress(ctx, "done", true); err != nil {
				t.Fatalf("SetOnboardingProgress(complete): %v", err)
			}
			done, err = store.IsOnboardingComplete(ctx)
			if err != nil {
				t.Fatalf("IsOnboardingComplete after complete: %v", err)
			}
			if !done {
				t.Fatalf("after complete: want complete, got incomplete")
			}
		})
	}
}
