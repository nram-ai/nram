package provider

import "testing"

func TestBuildMessages(t *testing.T) {
	const sys = "You are a fact extraction engine."
	const usr = "Text:\nhello"

	t.Run("sends system and user separately", func(t *testing.T) {
		msgs := BuildMessages(sys, usr)
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
		msgs := BuildMessages("", usr)
		if len(msgs) != 1 || msgs[0].Role != "user" || msgs[0].Content != usr {
			t.Errorf("empty system should yield a single user message, got %+v", msgs)
		}
	})
}
