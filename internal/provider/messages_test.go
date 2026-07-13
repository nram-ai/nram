package provider

import (
	"strings"
	"testing"
)

func TestBuildMessages(t *testing.T) {
	const sys = "You are a fact extraction engine."
	const usr = "Text:\nhello"

	t.Run("sends system and user separately", func(t *testing.T) {
		msgs := buildMessages(sys, usr)
		if len(msgs) != 2 {
			t.Fatalf("want 2 messages, got %d", len(msgs))
		}
		if msgs[0].Role != "system" || msgs[0].Content != sys {
			t.Errorf("system message = %+v", msgs[0])
		}
		if msgs[1].Role != "user" || msgs[1].Content != usr {
			t.Errorf("user message = %+v", msgs[1])
		}
	})

	t.Run("empty system yields one user message", func(t *testing.T) {
		msgs := buildMessages("", usr)
		if len(msgs) != 1 || msgs[0].Role != "user" || msgs[0].Content != usr {
			t.Errorf("empty system should yield a single user message, got %+v", msgs)
		}
	})
}

func TestBuildGuardedMessages(t *testing.T) {
	const sys = "You are a fact extraction engine."
	const usr = "Text:\nhello"

	t.Run("byte-identical to manual GuardedSystem pairing", func(t *testing.T) {
		got := BuildGuardedMessages(sys, usr)
		want := buildMessages(GuardedSystem(sys), usr)
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("message[%d] = %+v, want %+v", i, got[i], want[i])
			}
		}
		// The system half carries the untrusted-data directive.
		if !strings.HasPrefix(got[0].Content, UntrustedDataDirective) {
			t.Errorf("system message does not begin with the directive: %q", got[0].Content)
		}
		if got[1].Content != usr {
			t.Errorf("user message = %q, want %q", got[1].Content, usr)
		}
	})

	t.Run("empty system collapses like BuildMessages", func(t *testing.T) {
		// GuardedSystem("") is non-empty (it is the directive), so the result is
		// still a two-message pair — identical to the manual form.
		got := BuildGuardedMessages("", usr)
		want := buildMessages(GuardedSystem(""), usr)
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("BuildGuardedMessages(\"\", usr) = %+v, want %+v", got, want)
		}
	})
}

func TestGuardedPromptText(t *testing.T) {
	const sys = "SYSTEM"
	const usr = "USER"
	got := GuardedPromptText(sys, usr)
	want := GuardedSystem(sys) + PromptSplitSeparator + usr
	if got != want {
		t.Errorf("GuardedPromptText = %q, want %q", got, want)
	}
	if !strings.HasPrefix(got, UntrustedDataDirective) {
		t.Errorf("GuardedPromptText does not begin with the directive: %q", got)
	}
}
