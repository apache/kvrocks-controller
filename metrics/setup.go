/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type performanceMetrics struct {
	// HTTP performance metrics (populated by middleware)
	Latencies        *prometheus.HistogramVec
	HTTPCodes        *prometheus.CounterVec
	Payload          *prometheus.CounterVec
	HTTPServerPanics *prometheus.CounterVec

	// HA voting and failover metrics
	//
	// FailoverProposals counts every time the coordinateLoop dequeues a
	// proposal and calls RequestVotes, regardless of outcome.
	FailoverProposals *prometheus.CounterVec // labels: namespace, cluster
	// FailoverCompleted counts successful failovers (UpdateCluster persisted).
	FailoverCompleted *prometheus.CounterVec // labels: namespace, cluster
	// FailoverBlocked counts proposals that were blocked before promotion.
	// reason: "peer_voted_no" | "peer_unreachable" | "vote_error"
	FailoverBlocked *prometheus.CounterVec // labels: namespace, cluster, reason
	// VoteRoundDurationMs is the wall-clock duration of one RequestVotes call.
	// result: "approved" | "blocked" | "error"
	VoteRoundDurationMs *prometheus.HistogramVec // labels: namespace, cluster, result
	// NodeFailureCount is the current consecutive probe-failure count for each
	// kvrocks node.  Useful for "approaching threshold" alerts.
	NodeFailureCount *prometheus.GaugeVec // labels: namespace, cluster, node_id
	// ProbeFailures counts every individual probe failure, enabling rate-based
	// alerting ("node unreachable right now") independently of the failure-count
	// threshold used for failover decisions.
	// is_master: "true" | "false" — master failures warrant stricter alert thresholds.
	ProbeFailures *prometheus.CounterVec // labels: namespace, cluster, node_id, is_master
	// ActivePeersCount is the number of live peer controllers visible to this node
	// at the time of the most recent vote round.  Drops to 0 in single-node mode.
	// Alert when this falls below the expected cluster size — the controller cluster
	// has lost redundancy even if kvrocks failover still works.
	ActivePeersCount *prometheus.GaugeVec // labels: (none — node-scoped)
}

var _metrics *performanceMetrics

const (
	_namespace = "kvrocks"
	_subsystem = "controller"
)

// NewHistogramHelper was used to fast create and register prometheus histogram metric
func NewHistogramHelper(ns, subsystem, name string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	ns = strings.ReplaceAll(ns, "-", "_")
	subsystem = strings.ReplaceAll(subsystem, "-", "_")
	name = strings.ReplaceAll(name, "-", "_")
	opts := prometheus.HistogramOpts{}
	opts.Namespace = ns
	opts.Subsystem = subsystem
	opts.Name = name
	opts.Help = name
	opts.Buckets = buckets
	histogram := prometheus.NewHistogramVec(opts, labels)
	prometheus.MustRegister(histogram)
	return histogram
}

// NewGaugeHelper creates and registers a prometheus gauge metric.
func NewGaugeHelper(ns, subsystem, name string, labels ...string) *prometheus.GaugeVec {
	ns = strings.ReplaceAll(ns, "-", "_")
	subsystem = strings.ReplaceAll(subsystem, "-", "_")
	name = strings.ReplaceAll(name, "-", "_")
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Subsystem: subsystem,
		Name:      name,
		Help:      name,
	}, labels)
	prometheus.MustRegister(g)
	return g
}

// NewCounterHelper was used to fast create and register prometheus counter metric
func NewCounterHelper(ns, subsystem, name string, labels ...string) *prometheus.CounterVec {
	ns = strings.ReplaceAll(ns, "-", "_")
	subsystem = strings.ReplaceAll(subsystem, "-", "_")
	opts := prometheus.CounterOpts{}
	opts.Namespace = ns
	opts.Subsystem = subsystem
	opts.Name = name
	opts.Help = name
	counters := prometheus.NewCounterVec(opts, labels)
	prometheus.MustRegister(counters)
	return counters
}

func setupMetrics() {
	labels := []string{"host", "uri", "method", "code"}
	buckets := prometheus.ExponentialBuckets(1, 2, 16)
	newHistogram := func(name string, labels ...string) *prometheus.HistogramVec {
		return NewHistogramHelper(_namespace, _subsystem, name, buckets, labels...)
	}
	newCounter := func(name string, labels ...string) *prometheus.CounterVec {
		return NewCounterHelper(_namespace, _subsystem, name, labels...)
	}
	newGauge := func(name string, labels ...string) *prometheus.GaugeVec {
		return NewGaugeHelper(_namespace, _subsystem, name, labels...)
	}
	voteBuckets := prometheus.ExponentialBuckets(1, 2, 12) // 1ms … 4096ms
	_metrics = &performanceMetrics{
		Latencies: newHistogram("request_latency", labels...),
		HTTPCodes: newCounter("http_code", labels...),
		Payload:   newCounter("http_payload", labels...),

		ActivePeersCount:    newGauge("active_peers_count"),
		ProbeFailures:       newCounter("probe_failures_total", "namespace", "cluster", "node_id", "is_master"),
		FailoverProposals:   newCounter("failover_proposals_total", "namespace", "cluster"),
		FailoverCompleted:   newCounter("failover_completed_total", "namespace", "cluster"),
		FailoverBlocked:     newCounter("failover_blocked_total", "namespace", "cluster", "reason"),
		VoteRoundDurationMs: NewHistogramHelper(_namespace, _subsystem, "vote_round_duration_ms", voteBuckets, "namespace", "cluster", "result"),
		NodeFailureCount:    newGauge("node_failure_count", "namespace", "cluster", "node_id"),
	}
}

func Get() *performanceMetrics {
	return _metrics
}

func init() {
	setupMetrics()
}
