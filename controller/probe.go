package controller

import (
	"errors"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
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
	ID   string
	Addr string
}

type Probe struct {
	namespace string
	cluster   string
	storage   *storage.Storage
	failOver  *failover.FailOver

	stopCh chan struct{}
}

func NewProbe(ns, cluster string, storage *storage.Storage, failOver *failover.FailOver) *Probe {
	return &Probe{
		namespace: ns,
		cluster:   cluster,
		storage:   storage,
		failOver:  failOver,
		stopCh:    make(chan struct{}),
	}
}

func (p *Probe) start() {
	go p.probe()
}

func (p *Probe) probe() {
	probeTicker := time.NewTicker(time.Duration(ProbeInterval) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			nodeCount := 0
			probeFailureNodes := 0
			olderVersionNodes := 0
			newerVersionNodes := 0
			var latestEpoch int64
			var latestNodeAddr string
			probeInfos := make(map[int64][]*NodeInfo)
			clusterInfo, err := p.storage.GetClusterCopy(p.namespace, p.cluster)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("Failed to get clusterInfo info")
				break
			}
			for index, shard := range clusterInfo.Shards {
				for _, node := range shard.Nodes {
					nodeCount++
					info, err := util.ClusterInfoCmd(node.Address)
					if err != nil {
						probeFailureNodes++
						if err.Error() != ErrClusterDown.Error() && err.Error() != ErrRestoringBackUp.Error() {
							_ = p.failOver.AddNode(p.namespace, p.cluster, index, node, failover.AutoType)
							logger.Get().With(
								zap.String("addr", node.Address),
								zap.Error(err),
							).Error("Failed to probe the node")
						} else {
							logger.Get().With(
								zap.String("addr", node.Address),
								zap.Error(err),
							).Error("Failed to get clusterInfo info")
						}
						continue
					}

					if info.ClusterMyEpoch > latestEpoch {
						latestEpoch = info.ClusterMyEpoch
						latestNodeAddr = node.Address
					}
					probeInfos[info.ClusterMyEpoch] = append(probeInfos[info.ClusterMyEpoch], &NodeInfo{
						ID:   node.ID,
						Addr: node.Address,
					})
				}
			}

			if latestEpoch > clusterInfo.Version {
				latestClusterStr, err := util.ClusterNodesCmd(latestNodeAddr)
				if err != nil {
					logger.Get().With(
						zap.Any("node", latestNodeAddr),
						zap.Error(err),
					).Error("Failed to get the latest cluster info")
					break
				}
				latestClusterInfo, err := metadata.ParserToCluster(latestClusterStr)
				if err != nil {
					logger.Get().With(
						zap.Error(err),
					).Error("parser highest version clusterInfo nodes command error")
					break
				}
				p.storage.UpdateCluster(p.namespace, p.cluster, latestClusterInfo)
				logger.Get().With(
					zap.Int64("latest_version", latestEpoch),
					zap.String("latest_node", latestNodeAddr),
				).Warn("Cluster info version is greater than the storage")
				break
			}

			clusterStr, err := clusterInfo.ToSlotString()
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("clusterInfo info to string error")
				break
			}
			for currentVersion, nodes := range probeInfos {
				if currentVersion == clusterInfo.Version {
					continue
				}
				if currentVersion > clusterInfo.Version {
					newerVersionNodes += len(nodes)
					logger.Get().With(
						zap.Int64("cluster_version", clusterInfo.Version),
						zap.Int64("newer_node_version", currentVersion),
						zap.Int("nodes_count", len(nodes)),
					).Warn("Node version is ahead the storage")
					continue
				}
				olderVersionNodes += len(nodes)
				for _, node := range nodes {
					logger.Get().With(
						zap.Int64("cluster_version", clusterInfo.Version),
						zap.Int64("node_version", currentVersion),
						zap.Any("node", node),
					).Warn("Node version behind the storage")
					if err := util.SyncClusterInfo2Node(node.Addr, node.ID, clusterStr, clusterInfo.Version); err != nil {
						logger.Get().With(
							zap.String("node", node.Addr),
							zap.Error(err),
						).Error("Failed to Sync cluster info to node")
					}
				}
			}
			logger.Get().With(
				zap.Int("nodes", nodeCount),
				zap.Int("failures", probeFailureNodes),
				zap.Int("ahead_nodes", newerVersionNodes),
				zap.Int("behind_nodes", olderVersionNodes),
				zap.String("custer", util.BuildClusterKey(p.namespace, p.cluster)),
			).Info("Finish cluster probe")

		case <-p.stopCh:
			return
		}
	}
}

// stop cluster probe
func (p *Probe) stop() {
	close(p.stopCh)
}
