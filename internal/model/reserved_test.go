package model

import "testing"

func TestIsReservedProjectSlug(t *testing.T) {
	reserved := []string{ReservedProjectSlugGlobal, ReservedProjectSlugAboutMe}
	for _, slug := range reserved {
		if !IsReservedProjectSlug(slug) {
			t.Errorf("expected %q to be reserved", slug)
		}
	}
	for _, slug := range []string{"", "myapp", "globally", "about", "about-me", "About_Me"} {
		if IsReservedProjectSlug(slug) {
			t.Errorf("expected %q to NOT be reserved", slug)
		}
	}
}

func TestReservedProjectBySlug(t *testing.T) {
	for _, rp := range ReservedProjects {
		got := ReservedProjectBySlug(rp.Slug)
		if got == nil {
			t.Fatalf("expected canonical entry for %q", rp.Slug)
		}
		if got.Name == "" || got.Description == "" {
			t.Errorf("%q must carry a canonical name and description", rp.Slug)
		}
		if got.Slug != rp.Slug {
			t.Errorf("lookup mismatch: want %q, got %q", rp.Slug, got.Slug)
		}
	}
	if ReservedProjectBySlug("not-reserved") != nil {
		t.Error("expected nil for a non-reserved slug")
	}
	// The two tiers must be distinct and present.
	if ReservedProjectSlugGlobal == ReservedProjectSlugAboutMe {
		t.Fatal("reserved slugs must be distinct")
	}
}
