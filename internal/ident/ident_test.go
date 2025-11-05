package ident_test

import (
	"slices"
	"testing"

	"github.com/mickamy/gostry/internal/ident"
)

func TestSplitQualified(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   string
		want []string
	}{
		{name: "simple", in: "orders", want: []string{"orders"}},
		{name: "schema qualified", in: "public.orders", want: []string{"public", "orders"}},
		{name: "quoted schema and space", in: `"Sales"."Order Detail"`, want: []string{"Sales", "Order Detail"}},
		{name: "dot inside quotes", in: `"Sales"."Order.Detail"`, want: []string{"Sales", "Order.Detail"}},
		{name: "escaped quote", in: `"Sales""Region"."Orders"`, want: []string{`Sales"Region`, "Orders"}},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ident.SplitQualified(tc.in)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("SplitQualified(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestHistoryParts(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name   string
		base   string
		suffix string
		want   []string
	}{
		{name: "schema qualified", base: "public.orders", suffix: "_history", want: []string{"public", "orders_history"}},
		{name: "simple table", base: "orders", suffix: "_history", want: []string{"orders_history"}},
		{name: "empty base", base: "", suffix: "_history", want: []string{"_history"}},
		{name: "quoted", base: `"Sales"."Orders"`, suffix: "_history", want: []string{"Sales", "Orders_history"}},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ident.HistoryParts(tc.base, tc.suffix)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("HistoryParts(%q,%q) = %#v, want %#v", tc.base, tc.suffix, got, tc.want)
			}
		})
	}
}

func TestQuoteQualified(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   []string
		want string
	}{
		{name: "simple", in: []string{"orders_history"}, want: `"orders_history"`},
		{name: "schema qualified", in: []string{"public", "orders_history"}, want: `"public"."orders_history"`},
		{name: "needs escaping", in: []string{`Order"Detail`}, want: `"Order""Detail"`},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ident.QuoteQualified(tc.in)
			if got != tc.want {
				t.Fatalf("QuoteQualified(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestQualifiedRegclassLiteral(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   []string
		want string
	}{
		{name: "simple", in: []string{"orders_history"}, want: `'"orders_history"'`},
		{name: "schema qualified", in: []string{"public", "orders_history"}, want: `'"public"."orders_history"'`},
		{name: "empty", in: nil, want: `''`},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ident.QualifiedRegclassLiteral(tc.in)
			if got != tc.want {
				t.Fatalf("QualifiedRegclassLiteral(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestBaseTableName(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple", in: "orders", want: "orders"},
		{name: "schema qualified", in: "public.orders", want: "orders"},
		{name: "quoted", in: `"Sales"."Orders"`, want: "Orders"},
		{name: "dot in quotes", in: `"Sales"."Order.Detail"`, want: "Order.Detail"},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ident.BaseTableName(tc.in)
			if got != tc.want {
				t.Fatalf("BaseTableName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
