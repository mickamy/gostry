package query

import (
	"regexp"
	"strings"
)

// DML describes a recognized data-changing statement.
type DML struct {
	Op           string // INSERT, UPDATE, DELETE
	Table        string // possibly schema-qualified
	HasReturning bool
}

var (
	reInsert    = regexp.MustCompile(`(?is)^\s*(?:with\b.*?\)\s*)?insert\s+into\s+([^\s(]+)`)
	reUpdate    = regexp.MustCompile(`(?is)^\s*(?:with\b.*?\)\s*)?update\s+([^\s]+)\s+set\b`)
	reDelete    = regexp.MustCompile(`(?is)^\s*(?:with\b.*?\)\s*)?delete\s+from\s+([^\s]+)`)
	reReturning = regexp.MustCompile(`(?is)\breturning\b`)
)

// ParseDML attempts to recognize a single top-level DML and return its metadata.
func ParseDML(q string) (DML, bool) {
	qs := strings.TrimSpace(q)
	if m := reInsert.FindStringSubmatch(qs); len(m) == 2 {
		return DML{Op: "INSERT", Table: normalizeIdent(m[1]), HasReturning: reReturning.MatchString(qs)}, true
	}
	if m := reUpdate.FindStringSubmatch(qs); len(m) == 2 {
		return DML{Op: "UPDATE", Table: normalizeIdent(m[1]), HasReturning: reReturning.MatchString(qs)}, true
	}
	if m := reDelete.FindStringSubmatch(qs); len(m) == 2 {
		return DML{Op: "DELETE", Table: normalizeIdent(m[1]), HasReturning: reReturning.MatchString(qs)}, true
	}
	return DML{}, false
}

func normalizeIdent(s string) string {
	// Strip trailing commas/aliases if present; keep schema qualification.
	s = strings.TrimSpace(s)
	// Remove alias after table name: "public.orders o" -> "public.orders"
	parts := strings.Fields(s)
	if len(parts) > 0 {
		return strings.Trim(parts[0], `"`)
	}
	return s
}
