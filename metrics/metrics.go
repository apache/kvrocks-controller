package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var PrometheusMetrics *Metrics

type Metrics struct {
	AllNodes          *prometheus.GaugeVec
	FailNodes         *prometheus.GaugeVec
	OlderVersionNodes *prometheus.GaugeVec
	NewerVersionNodes *prometheus.GaugeVec

	FailoverFailCount *prometheus.GaugeVec
	MigrateFailCount  *prometheus.GaugeVec

	SwitchToLeader prometheus.Counter
}

func MustRegisterMetrics() {
	PrometheusMetrics = &Metrics{}

	// initialize metrics
	PrometheusMetrics.AllNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "all_nodes",
			Help: "all nodes counts",
		},
		[]string{"namespace", "cluster"},
	)

	PrometheusMetrics.FailNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "fail_nodes",
			Help: "fail nodes counts",
		},
		[]string{"namespace", "cluster"},
	)

	PrometheusMetrics.OlderVersionNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "old_version_nodes",
			Help: "old topu version nodes counts",
		},
		[]string{"namespace", "cluster"},
	)

	PrometheusMetrics.NewerVersionNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "newver_version_nodes",
			Help: "biger topu version nodes counts",
		},
		[]string{"namespace", "cluster"},
	)

	PrometheusMetrics.FailoverFailCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "failover_fail_count",
			Help: "failover node fail counts",
		},
		[]string{"namespace", "cluster"},
	)

	PrometheusMetrics.MigrateFailCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "migrate_fail_count",
			Help: "migrate task fail counts",
		},
		[]string{"namespace", "cluster"},
	)

	PrometheusMetrics.SwitchToLeader = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "controller_switch_leader",
			Help: "controller switch leader counts",
		})

	// register metrics
	prometheus.MustRegister(PrometheusMetrics.AllNodes)
	prometheus.MustRegister(PrometheusMetrics.FailNodes)
	prometheus.MustRegister(PrometheusMetrics.OlderVersionNodes)
	prometheus.MustRegister(PrometheusMetrics.NewerVersionNodes)
	prometheus.MustRegister(PrometheusMetrics.FailoverFailCount)
	prometheus.MustRegister(PrometheusMetrics.MigrateFailCount)
	prometheus.MustRegister(PrometheusMetrics.SwitchToLeader)
}