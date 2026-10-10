// Package groupagg implements grouped, decomposable aggregates for distributed
// IDSS queries (Green Power use case).
//
// A grouped query has the form
//
//	@group(keys=<k>[,<k>...]; aggs=<a>[,<a>...]) <EQL query with a show clause>
//
// where a key is a 0-based result column index, optionally wrapped in a
// transform (decade(i) maps a year to "1980s"), and an aggregate is one of
//
//	count | sum(i) | min(i) | max(i) | avg(i) | exposure(li,yi,from,to)
//
// Every aggregate is decomposable: each peer computes a Partial per group from
// its own rows, partials from different partitions are combined with Merge,
// and only the originator calls Finalise. avg is carried as (sum, count) so the
// final value is Σsum/Σcount, never a mean of means. exposure(li,yi,from,to)
// adds length × years in service within [from, to] (km-years for failure
// rates).
//
// The package has no network or database dependencies.
package groupagg

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Prefix marks a grouped query.
const Prefix = "@group("

// KeySpec selects a result column used as part of the group key.
type KeySpec struct {
	Col       int
	Transform string // "" or "decade"
}

// AggSpec is one aggregate.
type AggSpec struct {
	Func     string // count, sum, min, max, avg, exposure
	Col      int    // value column (sum/min/max/avg) or length column (exposure)
	YearCol  int    // exposure only
	From, To int    // exposure only
}

// Spec is a parsed @group clause.
type Spec struct {
	Keys []KeySpec
	Aggs []AggSpec
}

// Partial holds the decomposable state of one group.
type Partial struct {
	Key    []string
	Values []float64
}

// Partials maps an encoded group key to its partial state.
type Partials map[string]*Partial

var (
	callRe = regexp.MustCompile(`^\s*([a-z]+)\s*(?:\(([^)]*)\))?\s*$`)
)

// IsGrouped reports whether the query uses the @group prefix.
func IsGrouped(query string) bool {
	return strings.HasPrefix(strings.TrimSpace(query), Prefix)
}

// Parse splits a grouped query into its Spec and the EQL query to run locally.
func Parse(query string) (Spec, string, error) {
	keysPart, aggsPart, eql, err := splitHeader(query)
	if err != nil {
		return Spec{}, "", err
	}
	var spec Spec
	for _, item := range splitTop(keysPart) {
		if item == "" {
			continue
		}
		k, err := parseKey(item)
		if err != nil {
			return Spec{}, "", err
		}
		spec.Keys = append(spec.Keys, k)
	}
	for _, item := range splitTop(aggsPart) {
		if item == "" {
			continue
		}
		a, err := parseAgg(item)
		if err != nil {
			return Spec{}, "", err
		}
		spec.Aggs = append(spec.Aggs, a)
	}
	if len(spec.Aggs) == 0 {
		return Spec{}, "", fmt.Errorf("grouped query needs at least one aggregate")
	}
	if eql == "" {
		return Spec{}, "", fmt.Errorf("grouped query has no EQL part")
	}
	return spec, eql, nil
}

// splitHeader extracts the keys list, the aggs list and the EQL tail, matching
// brackets so that aggregates such as exposure(2,3,2013,2023) are kept whole.
func splitHeader(query string) (string, string, string, error) {
	q := strings.TrimSpace(query)
	bad := fmt.Errorf("not a grouped query: expected %skeys=...; aggs=...) <EQL>", Prefix)
	if !strings.HasPrefix(q, Prefix) {
		return "", "", "", bad
	}
	depth, end := 0, -1
	for i := len(Prefix) - 1; i < len(q); i++ {
		if q[i] == '(' {
			depth++
		} else if q[i] == ')' {
			depth--
			if depth == 0 {
				end = i
				break
			}
		}
	}
	if end < 0 {
		return "", "", "", fmt.Errorf("grouped query: unbalanced brackets in %q", q)
	}
	inner, eql := q[len(Prefix):end], strings.TrimSpace(q[end+1:])
	parts := strings.SplitN(inner, ";", 2)
	if len(parts) != 2 {
		return "", "", "", bad
	}
	field := func(s, name string) (string, bool) {
		s = strings.TrimSpace(s)
		if !strings.HasPrefix(strings.ToLower(s), name) {
			return "", false
		}
		s = strings.TrimSpace(s[len(name):])
		if !strings.HasPrefix(s, "=") {
			return "", false
		}
		return strings.TrimSpace(s[1:]), true
	}
	keys, ok1 := field(parts[0], "keys")
	aggs, ok2 := field(parts[1], "aggs")
	if !ok1 || !ok2 {
		return "", "", "", bad
	}
	return keys, aggs, eql, nil
}

// splitTop splits on commas that are not inside parentheses.
func splitTop(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}

func parseInts(args string, n int, what string) ([]int, error) {
	parts := strings.Split(args, ",")
	if len(parts) != n {
		return nil, fmt.Errorf("%s expects %d argument(s), got %q", what, n, args)
	}
	out := make([]int, n)
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || v < 0 && i < 2 {
			return nil, fmt.Errorf("%s: invalid argument %q", what, p)
		}
		out[i] = v
	}
	return out, nil
}

func parseKey(item string) (KeySpec, error) {
	if v, err := strconv.Atoi(item); err == nil && v >= 0 {
		return KeySpec{Col: v}, nil
	}
	m := callRe.FindStringSubmatch(strings.ToLower(item))
	if m == nil || m[1] != "decade" {
		return KeySpec{}, fmt.Errorf("invalid key %q (use a column index or decade(i))", item)
	}
	v, err := parseInts(m[2], 1, "decade")
	if err != nil {
		return KeySpec{}, err
	}
	return KeySpec{Col: v[0], Transform: "decade"}, nil
}

func parseAgg(item string) (AggSpec, error) {
	m := callRe.FindStringSubmatch(strings.ToLower(item))
	if m == nil {
		return AggSpec{}, fmt.Errorf("invalid aggregate %q", item)
	}
	switch m[1] {
	case "count":
		if strings.TrimSpace(m[2]) != "" {
			return AggSpec{}, fmt.Errorf("count takes no argument")
		}
		return AggSpec{Func: "count"}, nil
	case "sum", "min", "max", "avg":
		v, err := parseInts(m[2], 1, m[1])
		if err != nil {
			return AggSpec{}, err
		}
		return AggSpec{Func: m[1], Col: v[0]}, nil
	case "exposure":
		v, err := parseInts(m[2], 4, "exposure")
		if err != nil {
			return AggSpec{}, err
		}
		if v[3] < v[2] {
			return AggSpec{}, fmt.Errorf("exposure: to (%d) before from (%d)", v[3], v[2])
		}
		return AggSpec{Func: "exposure", Col: v[0], YearCol: v[1], From: v[2], To: v[3]}, nil
	}
	return AggSpec{}, fmt.Errorf("unsupported aggregate %q", m[1])
}

// slots returns the number of float slots an aggregate occupies.
func (a AggSpec) slots() int {
	if a.Func == "avg" {
		return 2
	}
	return 1
}

// Width is the number of value slots of a Partial for this Spec.
func (s Spec) Width() int {
	n := 0
	for _, a := range s.Aggs {
		n += a.slots()
	}
	return n
}

func (s Spec) newValues() []float64 {
	v := make([]float64, s.Width())
	i := 0
	for _, a := range s.Aggs {
		switch a.Func {
		case "min":
			v[i] = math.Inf(1)
		case "max":
			v[i] = math.Inf(-1)
		}
		i += a.slots()
	}
	return v
}

// ToFloat converts an EliasDB attribute value to float64.
func ToFloat(v interface{}) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(x), 64)
	case nil:
		return 0, fmt.Errorf("nil value")
	}
	return strconv.ParseFloat(fmt.Sprint(v), 64)
}

// formatKeyValue renders a key cell; integral floats print without decimals so
// that 2020 and 2020.0 fall into the same group on every peer.
func formatKeyValue(v interface{}) string {
	if f, ok := v.(float64); ok && f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return fmt.Sprint(v)
}

func encodeKey(parts []string) string { return strings.Join(parts, "\x1f") }

// Local groups the rows of a local EQL result.
func Local(spec Spec, rows [][]interface{}) (Partials, error) {
	out := Partials{}
	for ri, row := range rows {
		key := make([]string, len(spec.Keys))
		for i, k := range spec.Keys {
			if k.Col >= len(row) {
				return nil, fmt.Errorf("row %d: key column %d out of range (%d columns)", ri, k.Col, len(row))
			}
			if k.Transform == "decade" {
				y, err := ToFloat(row[k.Col])
				if err != nil {
					return nil, fmt.Errorf("row %d: decade(%d): %v", ri, k.Col, err)
				}
				key[i] = fmt.Sprintf("%ds", int(y)/10*10)
			} else {
				key[i] = formatKeyValue(row[k.Col])
			}
		}
		ek := encodeKey(key)
		p, ok := out[ek]
		if !ok {
			p = &Partial{Key: key, Values: spec.newValues()}
			out[ek] = p
		}
		i := 0
		for _, a := range spec.Aggs {
			switch a.Func {
			case "count":
				p.Values[i]++
			case "sum", "min", "max", "avg":
				if a.Col >= len(row) {
					return nil, fmt.Errorf("row %d: column %d out of range", ri, a.Col)
				}
				x, err := ToFloat(row[a.Col])
				if err != nil {
					return nil, fmt.Errorf("row %d: %s(%d): %v", ri, a.Func, a.Col, err)
				}
				switch a.Func {
				case "sum":
					p.Values[i] += x
				case "min":
					p.Values[i] = math.Min(p.Values[i], x)
				case "max":
					p.Values[i] = math.Max(p.Values[i], x)
				case "avg":
					p.Values[i] += x
					p.Values[i+1]++
				}
			case "exposure":
				if a.Col >= len(row) || a.YearCol >= len(row) {
					return nil, fmt.Errorf("row %d: exposure column out of range", ri)
				}
				l, err1 := ToFloat(row[a.Col])
				y, err2 := ToFloat(row[a.YearCol])
				if err1 != nil || err2 != nil {
					return nil, fmt.Errorf("row %d: exposure: non-numeric length or year", ri)
				}
				p.Values[i] += Exposure(l, int(y), a.From, a.To)
			}
			i += a.slots()
		}
	}
	return out, nil
}

// Exposure returns length × number of calendar years in [from, to] during which
// an asset put in service in year inService was in operation.
func Exposure(length float64, inService, from, to int) float64 {
	start := from
	if inService > start {
		start = inService
	}
	years := to - start + 1
	if years <= 0 {
		return 0
	}
	return length * float64(years)
}

// Merge folds src into dst (in place).
func Merge(spec Spec, dst, src Partials) {
	for k, sp := range src {
		dp, ok := dst[k]
		if !ok {
			cp := &Partial{Key: append([]string(nil), sp.Key...), Values: append([]float64(nil), sp.Values...)}
			dst[k] = cp
			continue
		}
		i := 0
		for _, a := range spec.Aggs {
			switch a.Func {
			case "min":
				dp.Values[i] = math.Min(dp.Values[i], sp.Values[i])
			case "max":
				dp.Values[i] = math.Max(dp.Values[i], sp.Values[i])
			default: // count, sum, avg (sum and count), exposure
				for j := 0; j < a.slots(); j++ {
					dp.Values[i+j] += sp.Values[i+j]
				}
			}
			i += a.slots()
		}
	}
}

// Encode serialises partials as string rows: key cells followed by value slots.
// Floats use the shortest exact representation, so decoding is lossless.
func Encode(p Partials) [][]string {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([][]string, 0, len(p))
	for _, k := range keys {
		row := append([]string(nil), p[k].Key...)
		for _, v := range p[k].Values {
			row = append(row, strconv.FormatFloat(v, 'g', -1, 64))
		}
		out = append(out, row)
	}
	return out
}

// Decode parses rows produced by Encode.
func Decode(spec Spec, rows [][]string) (Partials, error) {
	nk, nv := len(spec.Keys), spec.Width()
	out := Partials{}
	for ri, row := range rows {
		if len(row) != nk+nv {
			return nil, fmt.Errorf("partial row %d: %d cells, want %d", ri, len(row), nk+nv)
		}
		vals := make([]float64, nv)
		for j := 0; j < nv; j++ {
			v, err := strconv.ParseFloat(row[nk+j], 64)
			if err != nil {
				return nil, fmt.Errorf("partial row %d: %v", ri, err)
			}
			vals[j] = v
		}
		key := append([]string(nil), row[:nk]...)
		ek := encodeKey(key)
		if _, dup := out[ek]; dup {
			return nil, fmt.Errorf("partial row %d: duplicate group %q", ri, strings.Join(key, "|"))
		}
		out[ek] = &Partial{Key: key, Values: vals}
	}
	return out, nil
}

// Finalise turns merged partials into result rows sorted by key. header holds
// the column labels of the local EQL result (used to name the output columns).
func Finalise(spec Spec, header []string, p Partials) ([]string, [][]string) {
	label := func(col int) string {
		if col < len(header) && header[col] != "" {
			return header[col]
		}
		return fmt.Sprintf("col%d", col)
	}
	var outHeader []string
	for _, k := range spec.Keys {
		if k.Transform != "" {
			outHeader = append(outHeader, fmt.Sprintf("%s(%s)", k.Transform, label(k.Col)))
		} else {
			outHeader = append(outHeader, label(k.Col))
		}
	}
	for _, a := range spec.Aggs {
		switch a.Func {
		case "count":
			outHeader = append(outHeader, "count")
		case "exposure":
			outHeader = append(outHeader, fmt.Sprintf("exposure(%s,%s,%d,%d)", label(a.Col), label(a.YearCol), a.From, a.To))
		default:
			outHeader = append(outHeader, fmt.Sprintf("%s(%s)", a.Func, label(a.Col)))
		}
	}
	rows := Encode(p) // sorted by key
	out := make([][]string, 0, len(rows))
	for _, r := range rows {
		ek := encodeKey(r[:len(spec.Keys)])
		vals := p[ek].Values
		row := append([]string(nil), r[:len(spec.Keys)]...)
		i := 0
		for _, a := range spec.Aggs {
			var v float64
			switch a.Func {
			case "avg":
				if vals[i+1] != 0 {
					v = vals[i] / vals[i+1]
				} else {
					v = math.NaN()
				}
			default:
				v = vals[i]
			}
			row = append(row, strconv.FormatFloat(v, 'g', -1, 64))
			i += a.slots()
		}
		out = append(out, row)
	}
	return outHeader, out
}
