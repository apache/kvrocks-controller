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
	probeInterval      = failover.PingInterval / 3
	defaultFailOverCnt = int64(3)
)

type nodeInfo struct {
	ID           string
	Epoch        int64
	FailureCount int64
}

type ClusterProbe struct {
	namespace string
	cluster   string
	storage   *storage.Storage
	failOver  *failover.FailOver
	nodes     map[string]*nodeInfo
	stopCh    chan struct{}
}

func NewProbe(ns, cluster string, storage *storage.Storage, failOver *failover.FailOver) *ClusterProbe {
	return &ClusterProbe{
		namespace: ns,
		cluster:   cluster,
		storage:   storage,
		failOver:  failOver,
		nodes:     make(map[string]*nodeInfo),
		stopCh:    make(chan struct{}),
	}
}

func (p *ClusterProbe) start() {
	go p.loop()
}

func (p *ClusterProbe) probe(cluster *metadata.Cluster) (*metadata.Cluster, error) {
	var latestEpoch int64
	var latestNodeAddr string

	for index, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			if _, ok := p.nodes[node.Address]; !ok {
				p.nodes[node.Address] = &nodeInfo{ID: node.ID}
			}
			info, err := util.ClusterInfoCmd(node.Address)
			if err != nil {
				if err.Error() != ErrClusterDown.Error() && err.Error() != ErrRestoringBackUp.Error() {
					p.nodes[node.Address].FailureCount += 1
					if p.nodes[node.Address].FailureCount >= defaultFailOverCnt {
						err = p.failOver.AddNode(p.namespace, p.cluster, index, node, failover.AutoType)
						logger.Get().With(
							zap.String("node", node.Address),
							zap.Error(err),
						).Error("Failed to add the node to fail over")
					} else {
						logger.Get().With(
							zap.String("node", node.Address),
							zap.Int64("failure_count", p.nodes[node.Address].FailureCount),
						).Warn("Failed to ping the node")
					}
				} else {
					logger.Get().With(
						zap.String("node", node.Address),
						zap.Error(err),
					).Error("Failed to get cluster info")
				}
				continue
			}

			if info.ClusterMyEpoch > latestEpoch {
				latestEpoch = info.ClusterMyEpoch
				latestNodeAddr = node.Address
			}
			p.nodes[node.Address].Epoch = info.ClusterMyEpoch
			p.nodes[node.Address].FailureCount = 0
		}
	}

	if latestEpoch > cluster.Version {
		latestClusterStr, err := util.ClusterNodesCmd(latestNodeAddr)
		if err != nil {
			return nil, err
		}
		latestClusterInfo, err := metadata.ParserToCluster(latestClusterStr)
		if err != nil {
			return nil, err
		}
		err = p.storage.UpdateCluster(p.namespace, p.cluster, latestClusterInfo)
		if err != nil {
			return nil, err
		}
		return latestClusterInfo, nil
	}
	return cluster, nil
}

func (p *ClusterProbe) loop() {
	probeTicker := time.NewTicker(time.Duration(probeInterval) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			clusterInfo, err := p.storage.GetClusterCopy(p.namespace, p.cluster)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("Failed to get cluster info")
				break
			}
			latestClusterInfo, err := p.probe(&clusterInfo)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("Failed to probe the cluster info")
				break
			}

			clusterStr, err := latestClusterInfo.ToSlotString()
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("clusterInfo info to string error")
				break
			}
			for nodeAddr, probeInfo := range p.nodes {
				epoch := probeInfo.Epoch
				if epoch == latestClusterInfo.Version {
					continue
				}
				if epoch > latestClusterInfo.Version {
					logger.Get().With(
						zap.Int64("cluster_version", latestClusterInfo.Version),
						zap.Int64("newer_node_version", epoch),
						zap.String("node", nodeAddr),
					).Warn("Node Epoch is ahead the storage")
				} else {
					logger.Get().With(
						zap.Int64("cluster_version", latestClusterInfo.Version),
						zap.Int64("node_version", epoch),
						zap.Any("node", nodeAddr),
					).Warn("Node Epoch behind the storage")

					if err := util.SyncClusterInfo2Node(
						nodeAddr,
						probeInfo.ID,
						clusterStr,
						latestClusterInfo.Version,
					); err != nil {
						logger.Get().With(
							zap.String("node", nodeAddr),
							zap.Error(err),
						).Error("Failed to Sync cluster info to node")
					}
				}
			}

		case <-p.stopCh:
			return
		}
	}
}

// stop cluster loop
func (p *ClusterProbe) stop() {
	close(p.stopCh)
}
