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
	Latencies        *prometheus.HistogramVec
	HTTPCodes        *prometheus.CounterVec
	Payload          *prometheus.CounterVec
	HTTPServerPanics *prometheus.CounterVec
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
	_metrics = &performanceMetrics{
		Latencies: newHistogram("request_latency", labels...),
		HTTPCodes: newCounter("http_code", labels...),
		Payload:   newCounter("http_payload", labels...),
	}
}

func Get() *performanceMetrics {
	return _metrics
}

func init() {
	setupMetrics()
}
