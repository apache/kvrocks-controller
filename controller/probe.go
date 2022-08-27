package controller

import (
	"errors"
	"fmt"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/metrics"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	ErrClusterDown     = errors.New("CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp = errors.New("LOADING kvrocks is restoring the db from backup")
)

var (
	ProbeInterval = failover.PingInterval / 3
)

type NodeInfo struct {
	Id   string
	Addr string
}

type Probe struct {
	namespace string
	cluster   string
	stor      *storage.Storage
	nfor      *failover.FailOver

	stopCh chan struct{}
}

func NewProbe(ns, cluster string, stor *storage.Storage, nfor *failover.FailOver) *Probe {
	return &Probe{
		namespace: ns,
		cluster:   cluster,
		stor:      stor,
		nfor:      nfor,
		stopCh:    make(chan struct{}),
	}
}

func (p *Probe) start() {
	go p.probe()
}

func (p *Probe) probe() {
	probeTicker := time.NewTimer(time.Duration(ProbeInterval) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			allNodes := 0
			probeFailureNodes := 0
			olderVersionNodes := 0
			newerVersionNodes := 0
			var highestVersion int64
			var highestAddress string
			probeInfos := make(map[int64][]*NodeInfo)
			cluster, err := p.stor.GetClusterCopy(p.namespace, p.cluster)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("get cluster form local error")
				break
			}
			for index, shard := range cluster.Shards {
				for _, node := range shard.Nodes {
					allNodes++
					info, err := util.ClusterInfoCmd(node.Address)
					if err != nil {
						probeFailureNodes++
						if err.Error() != ErrClusterDown.Error() && err.Error() != ErrRestoringBackUp.Error() {
							_ = p.nfor.AddNode(p.namespace, p.cluster, index, node, failover.AutoType)
							logger.Get().Warn("pfail node: " + node.Address)
						} else {
							logger.Get().With(
								zap.Error(err),
							).Error("cluster info error")
							continue
						}
					} else {
						if info.ClusterMyEpoch > highestVersion {
							highestVersion = info.ClusterMyEpoch
							highestAddress = node.Address
						}
						probeInfos[info.ClusterMyEpoch] = append(probeInfos[info.ClusterMyEpoch],
							&NodeInfo{
								Id:   node.ID,
								Addr: node.Address,
							})
					}
				}
			}

			clusterNewest, err := p.stor.GetClusterCopy(p.namespace, p.cluster)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("get cluster form local error")
			} else {
				cluster = clusterNewest
			}
			clusterVer := cluster.Version

			if highestVersion > clusterVer {
				clusterStr, err := util.ClusterNodesCmd(highestAddress)
				if err != nil {
					logger.Get().With(
						zap.Any("node", highestAddress),
						zap.Error(err),
					).Error("send cluster nodes to highest version node error")
					break
				}
				topo, err := metadata.ParserToCluster(clusterStr)
				if err != nil {
					logger.Get().With(
						zap.Error(err),
					).Error("parser highest version cluster nodes command error")
					break
				}
				p.stor.UpdateCluster(p.namespace, p.cluster, topo)
				logger.Get().With(
					zap.Int64("cluster_version", clusterVer),
					zap.Int64("highest_version", highestVersion),
					zap.String("highest_node", highestAddress),
				).Warn("node version ahead and update cluster info by node")
				break
			}

			clusterStr, err := cluster.ToSlotString()
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("cluster info to string error")
				break
			}
			for ver, nodes := range probeInfos {
				if ver == clusterVer {
					continue
				}
				if ver > clusterVer {
					newerVersionNodes += len(nodes)
					logger.Get().With(
						zap.Int64("cluster_version", clusterVer),
						zap.Int64("node_version", ver),
						zap.Int("nodes", len(nodes)),
					).Warn("node version ahead")
					continue
				}
				olderVersionNodes += len(nodes)
				for _, node := range nodes {
					logger.Get().With(
						zap.Int64("cluster_version", clusterVer),
						zap.Int64("node_version", ver),
						zap.Any("node", node),
					).Warn("node version behind")
					if err := util.SyncClusterInfo2Node(node.Addr, node.Id, clusterStr, clusterVer); err != nil {
						logger.Get().With(
							zap.Error(err),
						).Error("sync cluster info to node error " + node.Addr)
					}
				}
			}

			metrics.PrometheusMetrics.AllNodes.With(prometheus.Labels{"namespace": p.namespace, "cluster": p.cluster}).Set(float64(allNodes))
			metrics.PrometheusMetrics.FailNodes.With(prometheus.Labels{"namespace": p.namespace, "cluster": p.cluster}).Set(float64(probeFailureNodes))
			metrics.PrometheusMetrics.OlderVersionNodes.With(prometheus.Labels{"namespace": p.namespace, "cluster": p.cluster}).Set(float64(olderVersionNodes))
			metrics.PrometheusMetrics.NewerVersionNodes.With(prometheus.Labels{"namespace": p.namespace, "cluster": p.cluster}).Set(float64(newerVersionNodes))

			if newerVersionNodes != 0 || olderVersionNodes != 0 || probeFailureNodes != 0 {
				logInfo := fmt.Sprintf("%s probe info, all: %d, pfail: %d, ahead: %d, behind: %d",
					util.NsClusterJoin(p.namespace, p.cluster), allNodes, probeFailureNodes, newerVersionNodes, olderVersionNodes)
				logger.Get().Warn(logInfo)
			}
		case <-p.stopCh:
			return
		}
		probeTicker.Reset(time.Duration(ProbeInterval) * time.Second)
	}
}

// stop cluster probe
func (p *Probe) stop() {
	close(p.stopCh)
}
