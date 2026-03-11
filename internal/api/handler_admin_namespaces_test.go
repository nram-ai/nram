package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// --- mock dependencies ---

type mockNamespaceStore struct {
	tree []NamespaceNode
	err  error
}

func (m *mockNamespaceStore) GetNamespaceTree(_ context.Context) ([]NamespaceNode, error) {
	return m.tree, m.err
}

// --- tests ---

func TestAdminNamespacesTreeSuccess(t *testing.T) {
	rootID := uuid.New()
	org1ID := uuid.New()
	org2ID := uuid.New()
	user1ID := uuid.New()
	project1ID := uuid.New()

	store := &mockNamespaceStore{
		tree: []NamespaceNode{
			{
				ID:    rootID,
				Name:  "root",
				Slug:  "root",
				Kind:  "root",
				Path:  "/",
				Depth: 0,
				Children: []NamespaceNode{
					{
						ID:    org1ID,
						Name:  "org1",
						Slug:  "org1",
						Kind:  "organization",
						Path:  "/org1",
						Depth: 1,
						Children: []NamespaceNode{
							{
								ID:    user1ID,
								Name:  "user1",
								Slug:  "user1",
								Kind:  "user",
								Path:  "/org1/user1",
								Depth: 2,
								Children: []NamespaceNode{
									{
										ID:    project1ID,
										Name:  "project1",
										Slug:  "project1",
										Kind:  "project",
										Path:  "/org1/user1/project1",
										Depth: 3,
									},
								},
							},
						},
					},
					{
						ID:    org2ID,
						Name:  "org2",
						Slug:  "org2",
						Kind:  "organization",
						Path:  "/org2",
						Depth: 1,
					},
				},
			},
		},
	}

	h := NewAdminNamespacesHandler(NamespaceAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/namespaces/tree", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp namespaceTreeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Tree) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(resp.Tree))
	}

	root := resp.Tree[0]
	if root.Kind != "root" {
		t.Errorf("expected root kind, got %q", root.Kind)
	}
	if root.Depth != 0 {
		t.Errorf("expected root depth 0, got %d", root.Depth)
	}
	if root.ID != rootID {
		t.Errorf("expected root ID %s, got %s", rootID, root.ID)
	}

	if len(root.Children) != 2 {
		t.Fatalf("expected 2 children of root, got %d", len(root.Children))
	}

	org1 := root.Children[0]
	if org1.Kind != "organization" {
		t.Errorf("expected organization kind, got %q", org1.Kind)
	}
	if org1.Name != "org1" {
		t.Errorf("expected org1, got %q", org1.Name)
	}
	if org1.Depth != 1 {
		t.Errorf("expected depth 1, got %d", org1.Depth)
	}

	if len(org1.Children) != 1 {
		t.Fatalf("expected 1 child of org1, got %d", len(org1.Children))
	}

	user1 := org1.Children[0]
	if user1.Kind != "user" {
		t.Errorf("expected user kind, got %q", user1.Kind)
	}
	if user1.Path != "/org1/user1" {
		t.Errorf("expected path /org1/user1, got %q", user1.Path)
	}

	if len(user1.Children) != 1 {
		t.Fatalf("expected 1 child of user1, got %d", len(user1.Children))
	}

	project1 := user1.Children[0]
	if project1.Kind != "project" {
		t.Errorf("expected project kind, got %q", project1.Kind)
	}
	if project1.Depth != 3 {
		t.Errorf("expected depth 3, got %d", project1.Depth)
	}
	if project1.Path != "/org1/user1/project1" {
		t.Errorf("expected path /org1/user1/project1, got %q", project1.Path)
	}

	org2 := root.Children[1]
	if org2.Kind != "organization" {
		t.Errorf("expected organization kind, got %q", org2.Kind)
	}
	if org2.Name != "org2" {
		t.Errorf("expected org2, got %q", org2.Name)
	}
	if len(org2.Children) != 0 {
		t.Errorf("expected 0 children for org2, got %d", len(org2.Children))
	}
}

func TestAdminNamespacesTreeEmpty(t *testing.T) {
	store := &mockNamespaceStore{tree: nil}
	h := NewAdminNamespacesHandler(NamespaceAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/namespaces/tree", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp namespaceTreeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Tree == nil {
		t.Fatal("expected non-nil tree slice")
	}
	if len(resp.Tree) != 0 {
		t.Errorf("expected empty tree, got %d nodes", len(resp.Tree))
	}
}

func TestAdminNamespacesTreeStoreError(t *testing.T) {
	store := &mockNamespaceStore{err: errors.New("database unavailable")}
	h := NewAdminNamespacesHandler(NamespaceAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/namespaces/tree", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp["error"] == "" {
		t.Error("expected error message in response")
	}
}

func TestAdminNamespacesTreeWrongMethod(t *testing.T) {
	store := &mockNamespaceStore{}
	h := NewAdminNamespacesHandler(NamespaceAdminConfig{Store: store})

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/v1/admin/namespaces/tree", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", method, w.Code)
		}
	}
}
