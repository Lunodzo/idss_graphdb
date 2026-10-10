// Central baseline: loads one partition file (or baseline_all.json) into a
// single EliasDB instance and runs the reliability report queries, printing
// result counts and execution time. Used as the reference for the P2P runs.
//
// Usage: go run ./tools/baseline -f data/baseline_all.json [-mem]
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/krotik/eliasdb/eql"
	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/graphstorage"

	"idss/graphdb/broadcast"
)

func run(gm *graph.Manager, q string) ([][]interface{}, []string, time.Duration) {
	t0 := time.Now()
	res, err := eql.RunQuery("baseline", "main", q, gm)
	if err != nil {
		fmt.Fprintln(os.Stderr, "query error:", q, err)
		os.Exit(1)
	}
	return res.Rows(), res.Header().Labels(), time.Since(t0)
}

func main() {
	file := flag.String("f", "baseline_all.json", "partition or baseline JSON file")
	flag.Parse()

	gs := graphstorage.NewMemoryGraphStorage("baseline")
	gm := graph.NewGraphManager(gs)
	t0 := time.Now()
	if err := broadcast.LoadGraphData(*file, "", gs, gm); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("load: %v\n", time.Since(t0))

	// Q1: failures per year
	rows, _, d := run(gm, "get FailureEvent show failureYear")
	perYear := map[string]int{}
	for _, r := range rows {
		perYear[fmt.Sprint(r[0])]++
	}
	keys := make([]string, 0, len(perYear))
	for k := range perYear {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("Q1 (%v):", d)
	for _, k := range keys {
		fmt.Printf(" %s=%d", k, perYear[k])
	}
	fmt.Println()

	// Q2/Q3: failures per DSO and cause (3 hops: FailureEvent->ACLineSegment->AssetContainer->Organisation)
	q := `get FailureEvent traverse failure:AFFECTS:asset:ACLineSegment
	        traverse segment:CONTAINS:container:AssetContainer
	          traverse asset:OWNS:owner:Organisation end end end
	      show 4:n:name, 1:n:faultCause`
	rows, _, d = run(gm, q)
	perDSO := map[string]int{}
	for _, r := range rows {
		if r[1] != nil {
			perDSO[fmt.Sprintf("%v|%v", r[0], r[1])]++
		}
	}
	keys = keys[:0]
	for k := range perDSO {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("Q3 (%v, %d rows):", d, len(rows))
	for _, k := range keys {
		fmt.Printf(" %s=%d", k, perDSO[k])
	}
	fmt.Println()
}
