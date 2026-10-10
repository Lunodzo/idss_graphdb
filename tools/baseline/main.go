// Central baseline: loads all partitions (baseline_all.json) into one EliasDB
// instance and runs the Green Power report queries with the same parsing, join
// matching and aggregation code as the overlay (broadcast.RunGroupedOnGraph).
// Results are written in the client's JSON format so that
// experiments/gp/check_ground_truth.py checks them like overlay results, and
// one timing row per query and repetition is appended to a CSV file.
//
// Usage:
//
//	go run ./tools/baseline -f data/s1/baseline_all.json \
//	    [-queries experiments/gp/queries.tsv] [-repeats 5] [-out dir] [-csv file] [-label s1]
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/graphstorage"

	"idss/graphdb/broadcast"
)

type query struct{ id, text string }

// readQueries parses "<id>\t<query>" lines; blank lines and # comments are skipped.
func readQueries(path string) ([]query, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var qs []query
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		id, text, ok := strings.Cut(line, "\t")
		if !ok {
			return nil, fmt.Errorf("%s: line without tab: %q", path, line)
		}
		qs = append(qs, query{strings.TrimSpace(id), strings.TrimSpace(text)})
	}
	return qs, sc.Err()
}

func main() {
	file := flag.String("f", "baseline_all.json", "baseline JSON file (all partitions)")
	queries := flag.String("queries", "experiments/gp/queries.tsv", "query file (<id><TAB><query>)")
	repeats := flag.Int("repeats", 5, "timed repetitions per query (after one warm-up run)")
	out := flag.String("out", "", "directory for result JSONs (<id>.json); empty: do not write")
	csvPath := flag.String("csv", "", "CSV file to append timing rows to; empty: stdout only")
	label := flag.String("label", "", "dataset label written to the CSV (default: directory of -f)")
	flag.Parse()
	if *label == "" {
		*label = filepath.Base(filepath.Dir(*file))
	}
	qs, err := readQueries(*queries)
	if err != nil {
		fail(err)
	}

	gs := graphstorage.NewMemoryGraphStorage("baseline")
	gm := graph.NewGraphManager(gs)
	t0 := time.Now()
	if err := broadcast.LoadGraphData(*file, "", gs, gm); err != nil {
		fail(err)
	}
	fmt.Printf("load %s: %v\n", *file, time.Since(t0))

	var csv *os.File
	if *csvPath != "" {
		_, statErr := os.Stat(*csvPath)
		if csv, err = os.OpenFile(*csvPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err != nil {
			fail(err)
		}
		defer csv.Close()
		if os.IsNotExist(statErr) {
			fmt.Fprintln(csv, "dataset,query,rep,ms,groups,events")
		}
	}
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			fail(err)
		}
	}

	for _, q := range qs {
		var header []string
		var rows [][]string
		var times []float64
		for rep := 0; rep <= *repeats; rep++ { // rep 0 is the warm-up run
			start := time.Now()
			h, r, events, err := broadcast.RunGroupedOnGraph(q.text, gm)
			ms := float64(time.Since(start).Microseconds()) / 1000
			if err != nil {
				fail(fmt.Errorf("%s: %v", q.id, err))
			}
			header, rows = h, r
			if rep > 0 {
				times = append(times, ms)
			}
			if csv != nil {
				fmt.Fprintf(csv, "%s,%s,%d,%.3f,%d,%d\n", *label, q.id, rep, ms, len(r), events)
			}
		}
		fmt.Printf("%-4s %4d groups  median %8.2f ms over %d runs\n", q.id, len(rows), median(times), len(times))
		if *out != "" {
			res := map[string]interface{}{"query": q.text, "resultCount": len(rows), "header": header, "rows": rows}
			b, _ := json.MarshalIndent(res, "", "  ")
			if err := os.WriteFile(filepath.Join(*out, q.id+".json"), b, 0o644); err != nil {
				fail(err)
			}
		}
	}
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	for i := 1; i < len(s); i++ { // insertion sort: a handful of values
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "baseline:", err)
	os.Exit(1)
}
