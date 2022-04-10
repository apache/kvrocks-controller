package controller

import (
	"context"
	"fmt"
	"sync"

	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
)

// Syncer would sync the cluster topo information
// to cluster nodes when it's changed.
type Syncer struct {
	stor     storage.Storage
	wg       sync.WaitGroup
	shutdown chan struct{}
	notifyCh chan storage.Event
}

func NewSyncer(stor storage.Storage) *Syncer {
	syncer := &Syncer{
		stor:     stor,
		shutdown: make(chan struct{}, 0),
		notifyCh: make(chan storage.Event, 8),
	}
	go syncer.loop()
	return syncer
}

func (syncer *Syncer) Notify(event *storage.Event) {
	syncer.notifyCh <- *event
}

func (syncer *Syncer) handleEvent(event *storage.Event) error {
	if event.Type == storage.EventCluster {
		return syncer.handleClusterEvent(event)
	}
	return syncer.handleShardEvent(event)
}

func (syncer *Syncer) handleClusterEvent(event *storage.Event) error {
	switch event.Command {
	case storage.CommandCreate:
		cluster, err := syncer.stor.GetCluster(event.Namespace, event.Cluster)
		if err != nil {
			return fmt.Errorf("failed to get cluster: %w", err)
		}
		return syncClusterInfoToAllNodes(context.Background(), cluster)
	default:
		return nil
	}
}

func (syncer *Syncer) handleShardEvent(event *storage.Event) error {
	return nil
}

func (syncer *Syncer) loop() {
	defer syncer.wg.Done()
	syncer.wg.Add(1)
	for {
		select {
		case event := <-syncer.notifyCh:
			if err := syncer.handleEvent(&event); err != nil {
				logger.Get().With(
					zap.Error(err),
					zap.Any("event", event),
				).Error("Failed to handle event")
			}
		case <-syncer.shutdown:
			return
		}
	}
}

func (syncer *Syncer) Close() {
	close(syncer.shutdown)
	close(syncer.notifyCh)
	syncer.wg.Wait()
}

func syncClusterInfoToNode(ctx context.Context, node *metadata.NodeInfo, clusterSlotsStr string, version int64) error {
	cli := redis.NewClient(&redis.Options{
		Addr: node.Address,
	})
	defer cli.Close()

	err := cli.Do(ctx, "CLUSTERX", "setnodeid", node.ID, version).Err()
	if err != nil {
		return fmt.Errorf("set node id: %w", err)
	}
	return cli.Do(ctx, "CLUSTERX", "setnodes", clusterSlotsStr, version).Err()
}

func syncClusterInfoToAllNodes(ctx context.Context, cluster *metadata.Cluster) error {
	// FIXME: should keep retry in separate routine to prevent occurring error
	// and cause update failure.
	clusterSlotsStr, err := cluster.ToSlotString()
	if err != nil {
		return err
	}
	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			err = syncClusterInfoToNode(ctx, &node, clusterSlotsStr, cluster.Version)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
