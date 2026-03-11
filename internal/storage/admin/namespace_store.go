package admin

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// NamespaceAdminStore implements api.NamespaceStore by querying the namespaces table.
type NamespaceAdminStore struct {
	db storage.DB
}

// NewNamespaceAdminStore creates a new NamespaceAdminStore.
func NewNamespaceAdminStore(db storage.DB) *NamespaceAdminStore {
	return &NamespaceAdminStore{db: db}
}

func (s *NamespaceAdminStore) GetNamespaceTree(ctx context.Context) ([]api.NamespaceNode, error) {
	query := `SELECT id, name, slug, kind, parent_id, path, depth
		FROM namespaces ORDER BY depth ASC, slug ASC`

	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("namespace tree query: %w", err)
	}
	defer rows.Close()

	type flatNode struct {
		ID       uuid.UUID
		Name     string
		Slug     string
		Kind     string
		ParentID *uuid.UUID
		Path     string
		Depth    int
	}

	var nodes []flatNode
	for rows.Next() {
		var n flatNode
		var idStr string
		var parentIDStr *string
		if err := rows.Scan(&idStr, &n.Name, &n.Slug, &n.Kind, &parentIDStr, &n.Path, &n.Depth); err != nil {
			return nil, fmt.Errorf("namespace tree scan: %w", err)
		}
		n.ID, _ = uuid.Parse(idStr)
		if parentIDStr != nil {
			pid, err := uuid.Parse(*parentIDStr)
			if err == nil {
				n.ParentID = &pid
			}
		}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("namespace tree iteration: %w", err)
	}

	// Build tree from flat list.
	nodeMap := make(map[uuid.UUID]*api.NamespaceNode, len(nodes))
	var roots []api.NamespaceNode

	for _, n := range nodes {
		node := api.NamespaceNode{
			ID:    n.ID,
			Name:  n.Name,
			Slug:  n.Slug,
			Kind:  n.Kind,
			Path:  n.Path,
			Depth: n.Depth,
		}
		nodeMap[n.ID] = &node

		if n.ParentID == nil {
			roots = append(roots, node)
		}
	}

	// Second pass to build children.
	for _, n := range nodes {
		if n.ParentID != nil {
			if parent, ok := nodeMap[*n.ParentID]; ok {
				child := nodeMap[n.ID]
				parent.Children = append(parent.Children, *child)
			} else {
				// Orphan — add as root.
				roots = append(roots, *nodeMap[n.ID])
			}
		}
	}

	// Update roots from the map (children may have been added).
	for i, r := range roots {
		if updated, ok := nodeMap[r.ID]; ok {
			roots[i] = *updated
		}
	}

	return roots, nil
}
