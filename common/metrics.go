/*
 * ! \file metrics.go
 * Prometheus metrics for IDSS query evaluation.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package common

import "github.com/prometheus/client_golang/prometheus"

var (
	QueryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "idss_query_total",
		Help: "Number of IDSS queries handled by query type.",
	}, []string{"type"})
	QueryDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "idss_query_duration_seconds",
		Help: "Duration of IDSS query handling at the receiving peer.",
	})
	QueryPeersResponded = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "idss_query_peers_responded",
		Help: "Number of peers that returned a query result.",
	})
	PolicyDecisionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "idss_policy_decision_total",
		Help: "Number of access-policy decisions by outcome.",
	}, []string{"decision"})
	RowsReturned = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "idss_rows_returned",
		Help: "Number of rows returned by an IDSS query.",
	})
)

func init() {
	prometheus.MustRegister(QueryTotal, QueryDuration, QueryPeersResponded, PolicyDecisionTotal, RowsReturned)
}
