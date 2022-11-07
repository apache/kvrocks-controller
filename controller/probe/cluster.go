package probe

import (
	"errors"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/controller/failover"
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

type Cluster struct {
	namespace     string
	cluster       string
	storage       *storage.Storage
	failOver      *failover.FailOver
	failureCounts map[string]int64
	stopCh        chan struct{}
}

func NewProbe(ns, cluster string, storage *storage.Storage, failOver *failover.FailOver) *Cluster {
	return &Cluster{
		namespace:     ns,
		cluster:       cluster,
		storage:       storage,
		failOver:      failOver,
		failureCounts: make(map[string]int64),
		stopCh:        make(chan struct{}),
	}
}

func (p *Cluster) start() {
	go p.loop()
}

func (p *Cluster) probe(cluster *metadata.Cluster) (*metadata.Cluster, error) {
	var latestEpoch int64
	var latestNodeAddr string

	currentClusterStr, _ := cluster.ToSlotString()
	for index, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			logger := logger.Get().With(
				zap.String("id", node.ID),
				zap.String("role", node.Role),
				zap.String("addr", node.Address),
			)
			if _, ok := p.failureCounts[node.Address]; !ok {
				p.failureCounts[node.Address] = 0
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
						logger.With(zap.Error(err)).Warn("Failed to re-sync the cluster info")
					}
					continue
				}
				p.failureCounts[node.Address] += 1
				if p.failureCounts[node.Address]%defaultFailOverCnt == 0 {
					err = p.failOver.AddNode(p.namespace, p.cluster, index, node, failover.AutoType)
					logger.With(zap.Error(err)).Warn("Add the node into the fail over candidates")
				} else {
					logger.With(
						zap.Error(err),
						zap.Int64("failure_count", p.failureCounts[node.Address]),
					).Warn("Failed to ping the node")
				}
				continue
			}
			if info.ClusterCurrentEpoch < cluster.Version {
				err := util.SyncClusterInfo2Node(node.Address, node.ID, currentClusterStr, cluster.Version)
				if err != nil {
					logger.With(
						zap.Error(err),
						zap.Int64("cluster_version", cluster.Version),
						zap.Int64("node_version", info.ClusterCurrentEpoch),
					).Info("Failed to sync the cluster info")
				}
			}

			if info.ClusterMyEpoch > latestEpoch {
				latestEpoch = info.ClusterMyEpoch
				latestNodeAddr = node.Address
			}
			p.failureCounts[node.Address] = 0
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

func (p *Cluster) loop() {
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
				).Error("Failed to get the cluster info from the storage")
				break
			}
			if _, err := p.probe(&clusterInfo); err != nil {
				logger.With(zap.Error(err)).Error("Failed to probe the cluster")
				break
			}
		case <-p.stopCh:
			return
		}
	}
}

func (p *Cluster) stop() {
	close(p.stopCh)
}
