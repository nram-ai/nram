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

func (s *NamespaceAdminStore) GetNamespaceTree(ctx context.Context, orgID *uuid.UUID) ([]api.NamespaceNode, error) {
	var query string
	var args []interface{}

	if orgID == nil {
		query = `SELECT id, name, slug, kind, parent_id, path, depth
			FROM namespaces ORDER BY depth ASC, slug ASC`
	} else {
		// Filter to namespaces under the org's namespace path (inclusive).
		if s.db.Backend() == storage.BackendPostgres {
			query = `SELECT id, name, slug, kind, parent_id, path, depth
				FROM namespaces
				WHERE path = (SELECT n.path FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
				   OR path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
				ORDER BY depth ASC, slug ASC`
		} else {
			query = `SELECT id, name, slug, kind, parent_id, path, depth
				FROM namespaces
				WHERE path = (SELECT n.path FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
				   OR path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
				ORDER BY depth ASC, slug ASC`
		}
		if s.db.Backend() == storage.BackendPostgres {
			args = []interface{}{orgID.String()}
		} else {
			args = []interface{}{orgID.String(), orgID.String()}
		}
	}

	rows, err := s.db.Query(ctx, query, args...)
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
		parsedID, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("namespace tree parse id: %w", err)
		}
		n.ID = parsedID
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
	roots := []api.NamespaceNode{}

	// Collect the set of returned node IDs so we can identify roots
	// within the filtered set (parent not in the result set).
	returnedIDs := make(map[uuid.UUID]bool, len(nodes))
	for _, n := range nodes {
		returnedIDs[n.ID] = true
	}

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

		if n.ParentID == nil || !returnedIDs[*n.ParentID] {
			roots = append(roots, node)
		}
	}

	// Second pass to build children.
	for _, n := range nodes {
		if n.ParentID != nil && returnedIDs[*n.ParentID] {
			if parent, ok := nodeMap[*n.ParentID]; ok {
				child := nodeMap[n.ID]
				parent.Children = append(parent.Children, *child)
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
