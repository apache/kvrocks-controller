package migrate

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

var (
	ErrEmptyTask             = errors.New("empty task")
	ErrMismatchNamespace     = errors.New("mismatch namespace")
	ErrMismatchCluster       = errors.New("mismatch cluster")
	ErrMismatchTasksID       = errors.New("mismatch task id")
	ErrMismatchMigrateSlot   = errors.New("mismatched slot")
	ErrTaskHasExisted        = errors.New("task has already existed")
	ErrUnknownTaskType       = errors.New("unknown task type")
	ErrTaskTimeout           = errors.New("task timeout")
	ErrSlotNoExists          = errors.New("source slot is not exists")
	ErrSlotFailed            = errors.New("migrate slot fail")
	ErrSlotConflicts         = errors.New("only one task is allowed at the same time")
	ErrSlotCompleted         = errors.New("task has been completed")
	ErrNotReady              = errors.New("in slave mode or is loading now")
	ErrAbortedMigrateTask    = errors.New("aborted migrate task")
	ErrAbortedMigrateSlot    = errors.New("aborted migrate slot")
	ErrAbortedMigrateRoutine = errors.New("aborted migrate routine")
)

var (
	TaskCheckInterval = 10 * time.Second
	TaskCheckMaxCount = 120
	SlotFailed        = "failed"
	SlotSuccess       = "success"
	SlotSleepInterval = time.Minute
)

const (
	TaskStatusPending = iota + 1
	TaskStatusMigrating
	TaskStatusSuccess
	TaskStatusFailed
)

type Migrator struct {
	storage *storage.Storage

	pendingTasks   map[string][]*storage.MigrationTask
	migratingTasks map[string]*storage.MigrationTask

	notifyCh chan *storage.MigrationTask
	stopCh   chan struct{}
	quitCh   chan struct{}

	rw    sync.RWMutex
	ready bool
}

func New(stor *storage.Storage) *Migrator {
	migrate := &Migrator{
		storage:        stor,
		pendingTasks:   make(map[string][]*storage.MigrationTask),
		migratingTasks: make(map[string]*storage.MigrationTask),
		notifyCh:       make(chan *storage.MigrationTask, 10),
		stopCh:         make(chan struct{}),
		quitCh:         make(chan struct{}),
	}
	return migrate
}

func (m *Migrator) Shutdown() {
	m.rw.Lock()
	defer m.rw.Unlock()
	if !m.ready {
		return
	}
	m.ready = false
	close(m.stopCh)
}

func (m *Migrator) loadTasks(ctx context.Context) ([]*storage.MigrationTask, error) {
	var migratingTask []*storage.MigrationTask

	namespaces, err := m.storage.ListNamespace(ctx)
	if err != nil {
		return nil, err
	}
	for _, namespace := range namespaces {
		clusters, err := m.storage.ListCluster(ctx, namespace)
		if err != nil {
			return nil, err
		}
		for _, cluster := range clusters {
			clusterKey := util.BuildClusterKey(namespace, cluster)
			pendingTasks, err := m.storage.GetPendingMigrateTasks(ctx, namespace, cluster)
			if err != nil {
				return nil, err
			}
			if len(pendingTasks) > 0 {
				m.pendingTasks[clusterKey] = pendingTasks
			}
			migratingTasks, err := m.storage.GetMigrateTask(ctx, namespace, cluster)
			if err != nil {
				return nil, err
			}
			if migratingTasks == nil {
				continue
			}
			existed, err := m.storage.IsMigrateHistoryExists(ctx, migratingTasks)
			if err != nil {
				return nil, err
			}
			if !existed && migratingTasks != nil {
				migratingTask = append(migratingTask, migratingTasks)
				pendingTasks = m.pendingTasks[clusterKey]
				m.pendingTasks[clusterKey] = append([]*storage.MigrationTask{migratingTasks}, pendingTasks...)
			} else if len(pendingTasks) > 0 {
				migratingTask = append(migratingTask, pendingTasks[0])
			}
		}
	}
	return migratingTask, nil
}

func (m *Migrator) Load(ctx context.Context) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	tasks, err := m.loadTasks(ctx)
	if err != nil {
		return err
	}

	go m.loop()

	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		defer wg.Done()
		for _, task := range tasks {
			m.notifyCh <- task
		}
	}()
	wg.Wait()

	m.stopCh = make(chan struct{})
	m.ready = true
	return nil
}

func (m *Migrator) AddTasks(ctx context.Context, tasks []*storage.MigrationTask) error {
	if !m.Ready() {
		return ErrNotReady
	}
	if len(tasks) == 0 {
		return ErrEmptyTask
	}
	namespace := tasks[0].Namespace
	cluster := tasks[0].Cluster
	taskID := tasks[0].TaskID
	for _, task := range tasks {
		if namespace != task.Namespace {
			return ErrMismatchNamespace
		}
		if cluster != task.Cluster {
			return ErrMismatchCluster
		}
		if taskID != task.TaskID {
			return ErrMismatchTasksID
		}
		sort.Slice(task.PlanSlots, func(i, j int) bool {
			return task.PlanSlots[i].Start < task.PlanSlots[j].Start
		})
		task.MigratingSlot = -1
		task.Status = TaskStatusPending
		task.PendingTime = time.Now().Unix()
	}
	has, err := m.storage.IsMigrateTaskExists(ctx, tasks[0].Namespace, tasks[0].Cluster, tasks[0].TaskID)
	if err != nil {
		return err
	}
	if has {
		return ErrTaskHasExisted
	}
	if err := m.addPendingTasks(ctx, namespace, cluster, tasks); err != nil {
		return err
	}
	m.notifyCh <- tasks[0]
	return nil
}

func (m *Migrator) GetMigrateTasks(ctx context.Context, namespace, cluster string, queryType string) ([]*storage.MigrationTask, error) {
	if !m.Ready() {
		return nil, ErrNotReady
	}
	name := util.BuildClusterKey(namespace, cluster)
	switch queryType {
	case "pending":
		m.rw.RLock()
		defer m.rw.RUnlock()
		if !m.hasPendingTasks(namespace, cluster) {
			return []*storage.MigrationTask{}, nil
		}
		return m.pendingTasks[name], nil
	case "migratingTasks":
		m.rw.RLock()
		defer m.rw.RUnlock()
		if !m.hasMigratingTask(ctx, namespace, cluster) {
			return []*storage.MigrationTask{}, nil
		}
		return []*storage.MigrationTask{m.migratingTasks[name]}, nil
	case "history":
		return m.storage.GetMigrateHistory(ctx, namespace, cluster)
	}
	return nil, ErrUnknownTaskType
}

func (m *Migrator) Ready() bool {
	m.rw.RLock()
	defer m.rw.RUnlock()
	return m.ready
}

func (m *Migrator) loop() {
	ctx := context.Background()
	for {
		select {
		case task := <-m.notifyCh:
			if m.hasMigratingTask(ctx, task.Namespace, task.Cluster) {
				continue
			}
			go m.startMigrating(ctx, task.Namespace, task.Cluster)
		case <-m.stopCh:
			return
		case <-m.quitCh:
			return
		}
	}
}

func (m *Migrator) startMigrating(ctx context.Context, namespace, cluster string) {
	for {
	loop:
		select {
		case <-m.quitCh:
			return
		case <-m.stopCh:
			return
		default:
		}
		task := m.consumePendingTask(ctx, namespace, cluster)
		if task == nil {
			time.Sleep(SlotSleepInterval)
			return
		}
		if err := m.addMigratingTask(ctx, task); err != nil {
			m.abortMigratingTask(ctx, task, err)
			continue
		}
		sourceNode, err := m.storage.GetMasterNode(ctx, namespace, cluster, task.Source)
		if err != nil {
			m.abortMigratingTask(ctx, task, err)
			continue
		}
		targetNode, err := m.storage.GetMasterNode(ctx, namespace, cluster, task.Target)
		if err != nil {
			m.abortMigratingTask(ctx, task, err)
			continue
		}
		isFirstSlot := true
		for _, slotRange := range task.PlanSlots {
			for slot := slotRange.Start; slot <= slotRange.Stop; slot++ {
				if task.MigratingSlot > slot {
					continue
				}
				time.Sleep(SlotSleepInterval)
				_ = m.storage.AddMigrateTask(ctx, task)
				err := m.migratingSlot(ctx, task, &sourceNode, &targetNode, slot, isFirstSlot)
				isFirstSlot = false
				if err == nil {
					continue
				}
				switch err.Error() {
				case ErrAbortedMigrateSlot.Error():
				case ErrAbortedMigrateTask.Error():
					goto loop
				case ErrAbortedMigrateRoutine.Error():
					return
				default:
					m.abortMigratingTask(ctx, task, err)
					goto loop
				}
			}
		}
		m.finishMigratingTask(ctx, task)
	}
}

func (m *Migrator) sendMigrateCommand(ctx context.Context, sourceNode, targetNode *metadata.NodeInfo, slot int) error {
	sourceClient, err := util.NewRedisClient(ctx, sourceNode.Addr)
	if err != nil {
		return err
	}
	return sourceClient.Do(ctx, "CLUSTERX", "migrate", strconv.Itoa(slot), targetNode.ID).Err()
}

func (m *Migrator) migratingSlot(
	ctx context.Context,
	task *storage.MigrationTask,
	source, target *metadata.NodeInfo,
	slot int, check bool) error {

	if check {
		clusterInfo, err := util.ClusterInfoCmd(ctx, source.Addr)
		if err != nil {
			m.abortMigratingTask(ctx, task, err)
			return ErrAbortedMigrateTask
		}
		if clusterInfo.MigratingSlot == task.MigratingSlot && clusterInfo.MigratingState == SlotSuccess {
			if err := m.storage.UpdateMigrateSlotInfo(ctx, task.Namespace, task.Cluster, task.Source, task.Target, task.MigratingSlot); err != nil {
				m.abortMigratingTask(ctx, task, err)
				return ErrAbortedMigrateTask
			}
			return ErrAbortedMigrateSlot
		}
	}
	task.MigratingSlot = slot

	exists, err := m.storage.HasSlot(ctx, task.Namespace, task.Cluster, task.Source, slot)
	if err != nil {
		m.abortMigratingTask(ctx, task, err)
		return ErrAbortedMigrateTask
	}
	if !exists {
		m.abortMigratingTask(ctx, task, ErrSlotNoExists)
		return ErrAbortedMigrateTask
	}

	err = m.sendMigrateCommand(context.Background(), source, target, slot)
	if err != nil {
		switch err.Error() {
		case ErrSlotConflicts.Error():
			// do nothing, will retry next
		case ErrSlotCompleted.Error():
			_ = m.storage.UpdateMigrateSlotInfo(ctx, task.Namespace,
				task.Cluster, task.Source, task.Target, slot)
		default:
			m.abortMigratingTask(ctx, task, err)
			return ErrAbortedMigrateTask
		}
	}

	count := 0
	checkResultTicker := time.NewTicker(TaskCheckInterval)
	defer checkResultTicker.Stop()
	for {
		if count == TaskCheckMaxCount {
			m.abortMigratingTask(ctx, task, ErrTaskTimeout)
			return ErrAbortedMigrateTask
		}
		select {
		case <-checkResultTicker.C:
			count++
			clusterInfo, err := util.ClusterInfoCmd(ctx, source.Addr)
			if err != nil {
				logger.Get().With(
					zap.String("node", source.Addr),
					zap.Error(err),
				).Error("Failed to get cluster info")
				continue
			}
			if clusterInfo.MigratingSlot != task.MigratingSlot {
				m.abortMigratingTask(ctx, task, ErrMismatchMigrateSlot)
				return ErrAbortedMigrateTask
			}
			switch clusterInfo.MigratingState {
			case SlotFailed:
				m.abortMigratingTask(ctx, task, ErrSlotFailed)
				return ErrAbortedMigrateTask
			case SlotSuccess:
				if err := m.storage.UpdateMigrateSlotInfo(ctx, task.Namespace, task.Cluster,
					task.Source, task.Target, task.MigratingSlot); err != nil {
					m.abortMigratingTask(ctx, task, err)
					return ErrAbortedMigrateTask
				}
				return nil
			}
		case <-m.stopCh:
			return ErrAbortedMigrateRoutine
		case <-m.quitCh:
			return ErrAbortedMigrateRoutine
		}
	}
}

func (m *Migrator) hasPendingTasks(namespace, cluster string) bool {
	m.rw.RLock()
	defer m.rw.RUnlock()
	_, ok := m.pendingTasks[util.BuildClusterKey(namespace, cluster)]
	return ok
}

func (m *Migrator) addPendingTasks(ctx context.Context, namespace, cluster string, tasks []*storage.MigrationTask) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	if err := m.storage.AddPendingMigrateTask(ctx, namespace, cluster, tasks); err != nil {
		return err
	}
	name := util.BuildClusterKey(namespace, cluster)
	m.pendingTasks[name] = append(m.pendingTasks[name], tasks...)
	return nil
}

func (m *Migrator) consumePendingTask(ctx context.Context, namespace, cluster string) *storage.MigrationTask {
	m.rw.Lock()
	defer m.rw.Unlock()
	name := util.BuildClusterKey(namespace, cluster)
	tasks, ok := m.pendingTasks[name]
	if !ok {
		return nil
	}
	task := tasks[0]
	if err := m.storage.RemovePendingMigrateTask(ctx, task); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to remove migrate task from storage")
	}
	if len(tasks) == 1 {
		delete(m.pendingTasks, name)
	} else {
		m.pendingTasks[name] = tasks[1:]
	}
	return task
}

func (m *Migrator) hasMigratingTask(_ context.Context, namespace, cluster string) bool {
	m.rw.RLock()
	defer m.rw.RUnlock()
	_, ok := m.migratingTasks[util.BuildClusterKey(namespace, cluster)]
	return ok
}

func (m *Migrator) addMigratingTask(ctx context.Context, task *storage.MigrationTask) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	task.Status = TaskStatusMigrating
	task.StartTime = time.Now().Unix()
	if err := m.storage.AddMigrateTask(ctx, task); err != nil {
		return err
	}
	m.migratingTasks[util.BuildClusterKey(task.Namespace, task.Cluster)] = task
	return nil
}

func (m *Migrator) removeMigratingTask(task *storage.MigrationTask) {
	m.rw.Lock()
	defer m.rw.Unlock()
	task.FinishTime = time.Now().Unix()
	delete(m.migratingTasks, util.BuildClusterKey(task.Namespace, task.Cluster))
}

func (m *Migrator) abortMigratingTask(ctx context.Context, task *storage.MigrationTask, err error) {
	task.Status = TaskStatusFailed
	task.ErrorDetail = err.Error()
	task.FinishTime = time.Now().Unix()
	_ = m.storage.AddMigrateHistory(ctx, task)
	m.removeMigratingTask(task)
	logger.Get().With(
		zap.Error(err),
		zap.Any("task", task),
	).Error("Aborted the migrate task")
}

// finishMigratingTask handler task status and push storage when task success
func (m *Migrator) finishMigratingTask(ctx context.Context, task *storage.MigrationTask) {
	task.Status = TaskStatusSuccess
	_ = m.storage.AddMigrateHistory(ctx, task)
	m.removeMigratingTask(task)
	logger.Get().With(
		zap.Any("task", task),
	).Info("Success to migrate the slot")
}
