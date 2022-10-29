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
	ErrClusterNotInitialized = errors.New("CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("LOADING kvrocks is restoring the db from backup")
)

var (
	probeInterval      = failover.PingInterval / 3
	defaultFailOverCnt = int64(15)
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
				if err.Error() == ErrRestoringBackUp.Error() {
					continue
				}
				if err.Error() == ErrClusterNotInitialized.Error() {
					// Maybe the node was restarted, just re-sync the cluster info
					clusterStr, _ := cluster.ToSlotString()
					err = util.SyncClusterInfo2Node(node.Address, node.ID, clusterStr, cluster.Version)
					if err != nil {
						logger.Get().With(
							zap.String("node", node.Address),
							zap.Error(err),
						).Warn("Failed to re-sync the cluster info")
					}
					continue
				}
				p.nodes[node.Address].FailureCount += 1
				if p.nodes[node.Address].FailureCount%defaultFailOverCnt == 0 {
					err = p.failOver.AddNode(p.namespace, p.cluster, index, node, failover.AutoType)
					logger.Get().With(
						zap.String("node", node.Address),
						zap.Error(err),
					).Warn("Add the node into the fail over candidates")
				} else {
					logger.Get().With(
						zap.Error(err),
						zap.String("node", node.Address),
						zap.Int64("failure_count", p.nodes[node.Address].FailureCount),
					).Warn("Failed to ping the node")
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
	logger := logger.Get().With(
		zap.String("namespace", p.namespace),
		zap.String("cluster", p.cluster),
	)
	probeTicker := time.NewTicker(time.Duration(probeInterval) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			clusterInfo, err := p.storage.GetClusterInfo(p.namespace, p.cluster)
			if err != nil {
				logger.With(
					zap.Error(err),
				).Error("Failed to get the cluster info")
				break
			}
			latestClusterInfo, err := p.probe(&clusterInfo)
			if err != nil {
				logger.With(
					zap.Error(err),
				).Error("Failed to probe the cluster info")
				break
			}

			clusterStr, err := latestClusterInfo.ToSlotString()
			if err != nil {
				logger.With(
					zap.Error(err),
				).Error("Failed to convert cluster slots to string")
				break
			}
			for nodeAddr, probeInfo := range p.nodes {
				epoch := probeInfo.Epoch
				if epoch == latestClusterInfo.Version {
					continue
				}
				if epoch > latestClusterInfo.Version {
					logger.With(
						zap.Int64("cluster_version", latestClusterInfo.Version),
						zap.Int64("newer_node_version", epoch),
						zap.String("node", nodeAddr),
					).Warn("Current node epoch is ahead the storage")
				} else {
					logger.With(
						zap.Int64("cluster_version", latestClusterInfo.Version),
						zap.Int64("node_version", epoch),
						zap.Any("node", nodeAddr),
					).Warn("Current node epoch is behind the storage")

					if err := util.SyncClusterInfo2Node(
						nodeAddr,
						probeInfo.ID,
						clusterStr,
						latestClusterInfo.Version,
					); err != nil {
						logger.With(
							zap.String("node", nodeAddr),
							zap.Error(err),
						).Error("Failed to sync the cluster info to node")
					}
				}
			}

		case <-p.stopCh:
			return
		}
	}
}

func (p *ClusterProbe) stop() {
	close(p.stopCh)
}
