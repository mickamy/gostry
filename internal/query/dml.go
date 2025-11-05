package query

import (
	"regexp"
	"strings"

	"github.com/mickamy/gostry/internal/ident"
)

// DML describes a recognized data-changing statement.
type DML struct {
	Op           string // INSERT, UPDATE, DELETE
	Table        string // possibly schema-qualified
	HasReturning bool
}

var (
	reInsert    = regexp.MustCompile(`(?is)^\s*(?:with\b.*?\)\s*)?insert\s+into\s+([^\s(]+)`)
	reUpdate    = regexp.MustCompile(`(?is)^\s*(?:with\b.*?\)\s*)?update\s+([^\s]+(?:\s+(?:as\s+)?[^\s]+)?)\s+set\b`)
	reDelete    = regexp.MustCompile(`(?is)^\s*(?:with\b.*?\)\s*)?delete\s+from\s+([^\s]+(?:\s+(?:as\s+)?[^\s]+)?)`)
	reReturning = regexp.MustCompile(`(?is)\breturning\b`)
)

// ParseDML attempts to recognize a single top-level DML and return its metadata.
func ParseDML(q string) (DML, bool) {
	qs := strings.TrimSpace(q)
	if m := reInsert.FindStringSubmatch(qs); len(m) == 2 {
		return DML{Op: "INSERT", Table: ident.StripAlias(m[1]), HasReturning: reReturning.MatchString(qs)}, true
	}
	if m := reUpdate.FindStringSubmatch(qs); len(m) == 2 {
		return DML{Op: "UPDATE", Table: ident.StripAlias(m[1]), HasReturning: reReturning.MatchString(qs)}, true
	}
	if m := reDelete.FindStringSubmatch(qs); len(m) == 2 {
		return DML{Op: "DELETE", Table: ident.StripAlias(m[1]), HasReturning: reReturning.MatchString(qs)}, true
	}
	return DML{}, false
}

// AppendReturningAll appends "RETURNING *" to the provided statement if non-empty.
// It preserves trailing semicolons by re-attaching them after the RETURNING clause.
func AppendReturningAll(q string) (string, bool) {
	trimmed := strings.TrimSpace(q)
	if trimmed == "" {
		return q, false
	}

	hasSemicolon := false
	for strings.HasSuffix(trimmed, ";") {
		hasSemicolon = true
		trimmed = strings.TrimSpace(trimmed[:len(trimmed)-1])
	}
	if trimmed == "" {
		return q, false
	}

	var b strings.Builder
	b.WriteString(trimmed)
	b.WriteString("\nRETURNING *")
	if hasSemicolon {
		b.WriteString(";")
	}
	return b.String(), true
}
