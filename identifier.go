package gostry

import (
	"strings"
)

// historyIdentifierParts returns qualified identifier parts with history suffix applied.
func historyIdentifierParts(base, suffix string) []string {
	parts := splitQualifiedIdentifier(base)
	if len(parts) == 0 {
		if suffix == "" {
			return nil
		}
		return []string{suffix}
	}
	out := make([]string, len(parts))
	copy(out, parts)
	out[len(out)-1] = out[len(out)-1] + suffix
	return out
}

// splitQualifiedIdentifier splits a potentially schema-qualified identifier into its parts.
func splitQualifiedIdentifier(ident string) []string {
	ident = strings.TrimSpace(ident)
	if ident == "" {
		return nil
	}
	var parts []string
	var buf strings.Builder
	inQuotes := false
	runes := []rune(ident)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch r {
		case '"':
			if inQuotes && i+1 < len(runes) && runes[i+1] == '"' {
				buf.WriteRune('"')
				i++
				continue
			}
			inQuotes = !inQuotes
		case '.':
			if inQuotes {
				buf.WriteRune(r)
				continue
			}
			part := strings.TrimSpace(buf.String())
			parts = append(parts, part)
			buf.Reset()
		default:
			buf.WriteRune(r)
		}
	}
	part := strings.TrimSpace(buf.String())
	parts = append(parts, part)
	return parts
}

// quoteQualifiedIdentifier renders qualified identifier parts as a SQL identifier.
func quoteQualifiedIdentifier(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = quoteIdentifier(p)
	}
	return strings.Join(quoted, ".")
}

func quoteIdentifier(part string) string {
	return `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
}

// qualifiedRegclassLiteral produces a regclass literal (e.g. 'public.table') with proper quoting.
func qualifiedRegclassLiteral(parts []string) string {
	if len(parts) == 0 {
		return "''"
	}
	ident := quoteQualifiedIdentifier(parts)
	return "'" + strings.ReplaceAll(ident, "'", "''") + "'"
}

// baseTableName returns the last segment of a qualified identifier.
func baseTableName(ident string) string {
	parts := splitQualifiedIdentifier(ident)
	if len(parts) == 0 {
		return strings.TrimSpace(ident)
	}
	return parts[len(parts)-1]
}
