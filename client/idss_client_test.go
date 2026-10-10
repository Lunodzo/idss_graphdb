package main

import "testing"

func TestSplitQueryAndTTL(t *testing.T) {
	for _, c := range []struct {
		in    string
		query string
		ttl   float64
		err   bool
	}{
		{"get Customer, 5", "get Customer", 5, false},
		{"get Customer", "get Customer", 1, false},
		{"@group(keys=0,1; aggs=count) get FailureEvent show 4:n:name, 1:n:faultCause, 10", "@group(keys=0,1; aggs=count) get FailureEvent show 4:n:name, 1:n:faultCause", 10, false},
		{"@group(keys=0,1; aggs=count) get FailureEvent show a, b", "@group(keys=0,1; aggs=count) get FailureEvent show a, b", 1, false},
		{"get Customer, 0", "", 0, true},
	} {
		q, ttl, err := splitQueryAndTTL(c.in)
		if (err != nil) != c.err || q != c.query || ttl != c.ttl {
			t.Errorf("splitQueryAndTTL(%q) = (%q, %v, %v), want (%q, %v, err=%v)", c.in, q, ttl, err, c.query, c.ttl, c.err)
		}
	}
}

func TestManifestSummary(t *testing.T) {
	got := manifestSummary([]ManifestEntry{{Partition: "dso01", Status: "ANSWERED"}, {Partition: "dso04", Status: "TIMEOUT"}})
	if got != "answered 1/2; timeout [dso04]" {
		t.Fatalf("summary = %q", got)
	}
}
