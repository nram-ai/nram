package logging

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

// FanoutHandler is the default slog handler. It forwards every record to a
// console handler (whose own level gating is preserved) and, when the database
// sink is enabled and the record meets the DB level, converts the record to a
// structured logging.Record and enqueues it on the async writer.
//
// Group and attribute state added via WithGroup/WithAttrs is tracked so the DB
// record carries the full structured context, nested under any open groups,
// exactly as the console output shows it.
type FanoutHandler struct {
	console slog.Handler
	cfg     *DBConfig
	writer  enqueuer
	goas    []groupOrAttrs
}

// groupOrAttrs is one frame of accumulated WithGroup/WithAttrs state: either an
// opened group name or a set of attrs that belong at the current depth.
type groupOrAttrs struct {
	group string
	attrs []slog.Attr
}

// NewFanoutHandler builds a handler teeing to console and (via writer) the DB
// sink gated by cfg. writer may be nil to disable DB capture entirely.
func NewFanoutHandler(console slog.Handler, cfg *DBConfig, writer enqueuer) *FanoutHandler {
	return &FanoutHandler{console: console, cfg: cfg, writer: writer}
}

// Enabled reports whether either destination wants a record at this level, so a
// DB-only level below the console level still gets handled.
func (h *FanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.console.Enabled(ctx, level) {
		return true
	}
	return h.writer != nil && h.cfg != nil && h.cfg.admits(level)
}

// Handle forwards to the console (respecting its own level) and, when the DB
// sink admits the level, enqueues a structured record.
func (h *FanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.console.Enabled(ctx, r.Level) {
		if err := h.console.Handle(ctx, r); err != nil {
			return err
		}
	}
	if h.writer != nil && h.cfg != nil && h.cfg.admits(r.Level) {
		h.writer.Enqueue(h.toRecord(r))
	}
	return nil
}

// WithAttrs returns a handler with the given attrs appended to both the console
// handler and the tracked DB state.
func (h *FanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	return h.clone(h.console.WithAttrs(attrs), groupOrAttrs{attrs: attrs})
}

// WithGroup returns a handler that nests subsequent attrs under name.
func (h *FanoutHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.clone(h.console.WithGroup(name), groupOrAttrs{group: name})
}

func (h *FanoutHandler) clone(console slog.Handler, ga groupOrAttrs) *FanoutHandler {
	goas := make([]groupOrAttrs, len(h.goas), len(h.goas)+1)
	copy(goas, h.goas)
	goas = append(goas, ga)
	return &FanoutHandler{console: console, cfg: h.cfg, writer: h.writer, goas: goas}
}

// toRecord builds the structured Record: applies accumulated group/attr state,
// merges the record's own attrs, then lifts out component and tenant ids.
func (h *FanoutHandler) toRecord(r slog.Record) Record {
	root := make(map[string]any, r.NumAttrs()+len(h.goas))
	cur := root
	for _, ga := range h.goas {
		if ga.group != "" {
			next := map[string]any{}
			cur[ga.group] = next
			cur = next
			continue
		}
		for _, a := range ga.attrs {
			addAttr(cur, a)
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		addAttr(cur, a)
		return true
	})

	ts := r.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	rec := Record{
		Time:    ts.UTC(),
		Level:   LevelName(r.Level),
		Message: r.Message,
		Attrs:   root,
	}
	// Component: a top-level attr wins; otherwise parse a "word:" message prefix.
	if c, ok := root[ComponentKey].(string); ok && c != "" {
		rec.Component = c
		delete(root, ComponentKey)
	} else if c := componentFromMessage(r.Message); c != "" {
		rec.Component = c
	}
	rec.ProjectID = uuidFrom(root, "project", "project_id")
	rec.NamespaceID = uuidFrom(root, "namespace", "namespace_id")
	rec.UserID = uuidFrom(root, "user", "user_id")
	return rec
}

// addAttr resolves an attr and stores it in m with its JSON-friendly value,
// recursing into groups (an empty-key group is inlined).
func addAttr(m map[string]any, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		if len(group) == 0 {
			return
		}
		if a.Key == "" {
			for _, ga := range group {
				addAttr(m, ga)
			}
			return
		}
		sub := map[string]any{}
		for _, ga := range group {
			addAttr(sub, ga)
		}
		m[a.Key] = sub
		return
	}
	m[a.Key] = attrValue(a.Value)
}

// attrValue maps a resolved slog.Value to a JSON-friendly Go value.
func attrValue(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time().UTC().Format(time.RFC3339Nano)
	default:
		return v.Any()
	}
}

// uuidFrom looks up the first of the given keys whose value parses as a UUID,
// accepting both string and uuid.UUID attr values. The key is left in the attrs
// map so the structured field is still visible in the record.
func uuidFrom(m map[string]any, keys ...string) *uuid.UUID {
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		switch t := v.(type) {
		case string:
			if id, err := uuid.Parse(t); err == nil {
				return &id
			}
		case uuid.UUID:
			if t != uuid.Nil {
				id := t
				return &id
			}
		}
	}
	return nil
}

// componentFromMessage extracts a leading "word:" prefix as the component, e.g.
// "enrichment: batch claimed" -> "enrichment". Returns "" when the prefix is
// not a single identifier token.
func componentFromMessage(msg string) string {
	i := strings.IndexByte(msg, ':')
	if i <= 0 {
		return ""
	}
	prefix := msg[:i]
	for _, c := range prefix {
		if c == '_' || c == '-' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		return ""
	}
	return prefix
}
