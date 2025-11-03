package gostry

// entry represents a captured change for a single row or statement.
type entry struct {
	table  string
	op     string
	sql    string
	args   []any
	before map[string]any // optional (DELETE/advanced UPDATE)
	after  map[string]any // optional (INSERT/UPDATE)
	meta   meta
}

// meta carries operational context for audit trails.
type meta struct {
	operator string
	traceID  string
	reason   string
}
