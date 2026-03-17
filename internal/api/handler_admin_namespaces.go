package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

// NamespaceNode represents a single node in the namespace hierarchy tree.
type NamespaceNode struct {
	ID       uuid.UUID       `json:"id"`
	Name     string          `json:"name"`
	Slug     string          `json:"slug"`
	Kind     string          `json:"kind"`
	Path     string          `json:"path"`
	Depth    int             `json:"depth"`
	Children []NamespaceNode `json:"children,omitempty"`
}

// NamespaceStore abstracts access to namespace tree data.
// When orgID is non-nil, the tree is filtered to the organization's subtree.
type NamespaceStore interface {
	GetNamespaceTree(ctx context.Context, orgID *uuid.UUID) ([]NamespaceNode, error)
}

// NamespaceAdminConfig holds dependencies for the admin namespaces handler.
type NamespaceAdminConfig struct {
	Store NamespaceStore
}

type namespaceTreeResponse struct {
	Tree []NamespaceNode `json:"tree"`
}

// NewAdminNamespacesHandler returns an http.HandlerFunc that serves the full
// namespace hierarchy tree via GET.
func NewAdminNamespacesHandler(cfg NamespaceAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "method not allowed",
			})
			return
		}

		orgID := resolveOrgScope(r)

		nodes, err := cfg.Store.GetNamespaceTree(r.Context(), orgID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve namespace tree",
			})
			return
		}

		if nodes == nil {
			nodes = []NamespaceNode{}
		}

		writeJSON(w, http.StatusOK, namespaceTreeResponse{Tree: nodes})
	}
}
