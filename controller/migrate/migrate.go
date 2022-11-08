package migrate

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
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
	TaskCheckInterval = 1
	TaskCheckMaxCount = 24 * 60 * 60
	SlotFail          = "failed"
	SlotSuccess       = "success"
	SlotSleepInterval = time.Minute
)

const (
	TaskStatusPending = iota + 1
	TaskStatusMigrating
	TaskStatusSuccess
	TaskStatusFailed
)

type Migrate struct {
	storage *storage.Storage

	pendingTasks   map[string][]*etcd.MigrateTask
	migratingTasks map[string]*etcd.MigrateTask

	notifyCh chan *etcd.MigrateTask
	stopCh   chan struct{}
	quitCh   chan struct{}

	rw    sync.RWMutex
	ready bool
}

func New(storage *storage.Storage) *Migrate {
	migrate := &Migrate{
		storage:        storage,
		pendingTasks:   make(map[string][]*etcd.MigrateTask),
		migratingTasks: make(map[string]*etcd.MigrateTask),
		notifyCh:       make(chan *etcd.MigrateTask, 10),
		stopCh:         make(chan struct{}),
		quitCh:         make(chan struct{}),
	}
	return migrate
}

func (m *Migrate) Shutdown() {
	m.rw.Lock()
	defer m.rw.Unlock()
	if !m.ready {
		return
	}
	m.ready = false
	close(m.stopCh)
}

func (m *Migrate) loadTasks() ([]*etcd.MigrateTask, error) {
	var migratingTask []*etcd.MigrateTask

	namespaces, err := m.storage.ListNamespace()
	if err != nil {
		return nil, err
	}
	for _, namespace := range namespaces {
		clusters, err := m.storage.ListCluster(namespace)
		if err != nil {
			return nil, err
		}
		for _, cluster := range clusters {
			clusterKey := util.BuildClusterKey(namespace, cluster)
			pendingTasks, err := m.storage.GetPendingMigrateTasks(namespace, cluster)
			if err != nil {
				return nil, err
			}
			if len(pendingTasks) > 0 {
				m.pendingTasks[clusterKey] = pendingTasks
			}
			migratingTasks, err := m.storage.ListMigrateTask(namespace, cluster)
			if err != nil {
				return nil, err
			}
			existed, err := m.storage.IsMigrateHistoryExists(migratingTasks)
			if err != nil {
				return nil, err
			}
			if !existed && migratingTasks != nil {
				migratingTask = append(migratingTask, migratingTasks)
				pendingTasks = m.pendingTasks[clusterKey]
				m.pendingTasks[clusterKey] = append([]*etcd.MigrateTask{migratingTasks}, pendingTasks...)
			} else if len(pendingTasks) > 0 {
				migratingTask = append(migratingTask, pendingTasks[0])
			}
		}
	}
	return migratingTask, nil
}

func (m *Migrate) Load() error {
	m.rw.Lock()
	defer m.rw.Unlock()
	if !m.storage.IsLeader() {
		return storage.ErrNoLeaderOrNotReady
	}
	tasks, err := m.loadTasks()
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

func (m *Migrate) AddTasks(tasks []*etcd.MigrateTask) error {
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
	has, err := m.storage.IsMigrateTaskExists(tasks[0].Namespace, tasks[0].Cluster, tasks[0].TaskID)
	if err != nil {
		return err
	}
	if has {
		return ErrTaskHasExisted
	}
	if err := m.addPendingTasks(namespace, cluster, tasks); err != nil {
		return err
	}
	m.notifyCh <- tasks[0]
	return nil
}

func (m *Migrate) GetMigrateTasks(namespace, cluster string, queryType string) ([]*etcd.MigrateTask, error) {
	if !m.Ready() {
		return nil, ErrNotReady
	}
	if !m.storage.IsLeader() {
		return nil, storage.ErrNoLeaderOrNotReady
	}
	name := util.BuildClusterKey(namespace, cluster)
	switch queryType {
	case "pending":
		m.rw.RLock()
		defer m.rw.RUnlock()
		if !m.hasPendingTasks(namespace, cluster) {
			return []*etcd.MigrateTask{}, nil
		}
		return m.pendingTasks[name], nil
	case "migratingTasks":
		m.rw.RLock()
		defer m.rw.RUnlock()
		if !m.hasMigratingTask(namespace, cluster) {
			return []*etcd.MigrateTask{}, nil
		}
		return []*etcd.MigrateTask{m.migratingTasks[name]}, nil
	case "history":
		return m.storage.GetMigrateHistory(namespace, cluster)
	}
	return nil, ErrUnknownTaskType
}

func (m *Migrate) Ready() bool {
	m.rw.RLock()
	defer m.rw.RUnlock()
	return m.ready
}

func (m *Migrate) loop() {
	for {
		if !m.storage.IsLeader() {
			time.Sleep(time.Duration(etcd.SessionTTL) * time.Second)
			continue
		}
		select {
		case task := <-m.notifyCh:
			if m.hasMigratingTask(task.Namespace, task.Cluster) {
				continue
			}
			go m.startMigrating(task.Namespace, task.Cluster)
		case <-m.stopCh:
			return
		case <-m.quitCh:
			return
		}
	}
}

func (m *Migrate) startMigrating(namespace, cluster string) {
	for {
	loop:
		select {
		case <-m.quitCh:
			return
		case <-m.stopCh:
			return
		default:
		}
		task := m.removePendingTask(namespace, cluster)
		if task == nil {
			time.Sleep(SlotSleepInterval)
			return
		}
		if err := m.addMigratingTask(task); err != nil {
			m.abortMigratingTask(task, err)
			continue
		}
		sourceNode, err := m.storage.GetMasterNode(namespace, cluster, task.Source)
		if err != nil {
			m.abortMigratingTask(task, err)
			continue
		}
		targetNode, err := m.storage.GetMasterNode(namespace, cluster, task.Target)
		if err != nil {
			m.abortMigratingTask(task, err)
			continue
		}
		cli, err := util.NewRedisClient(sourceNode.Address)
		if err != nil {
			m.abortMigratingTask(task, err)
			continue
		}
		firstMigrate := true
		for _, slotRange := range task.PlanSlots {
			for slot := slotRange.Start; slot <= slotRange.Stop; slot++ {
				if task.MigratingSlot > slot {
					continue
				}
				time.Sleep(SlotSleepInterval)
				_ = m.storage.AddMigrateTask(task)
				err := m.migratingSlot(cli, task, &sourceNode, &targetNode, slot, firstMigrate)
				firstMigrate = false
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
					m.abortMigratingTask(task, err)
					goto loop
				}
			}
		}
		m.finishMigratingTask(task)
	}
}

func (m *Migrate) migratingSlot(cli *redis.Client,
	task *etcd.MigrateTask,
	source, target *metadata.NodeInfo,
	slot int, check bool) error {

	if check {
		clusterInfo, err := util.ClusterInfoCmd(source.Address)
		if err != nil {
			m.abortMigratingTask(task, err)
			return ErrAbortedMigrateTask
		}
		if clusterInfo.MigratingSlot == task.MigratingSlot && clusterInfo.MigratingState == SlotSuccess {
			if err := m.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.MigratingSlot); err != nil {
				m.abortMigratingTask(task, err)
				return ErrAbortedMigrateTask
			}
			return ErrAbortedMigrateSlot
		}
	}
	task.MigratingSlot = slot

	exists, err := m.storage.HasSlot(task.Namespace, task.Cluster, task.Source, slot)
	if err != nil {
		m.abortMigratingTask(task, err)
		return ErrAbortedMigrateTask
	}
	if !exists {
		m.abortMigratingTask(task, ErrSlotNoExists)
		return ErrAbortedMigrateTask
	}

	err = cli.Do(context.Background(), "CLUSTERX", "migrate", strconv.Itoa(slot), target.ID).Err()
	if err != nil {
		switch err.Error() {
		case ErrSlotCompleted.Error():
			if err := m.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, slot); err != nil {
				m.abortMigratingTask(task, err)
				return ErrAbortedMigrateTask
			}
			return ErrAbortedMigrateSlot
		case ErrSlotConflicts.Error():
		default:
			m.abortMigratingTask(task, err)
			return ErrAbortedMigrateTask
		}
	}

	count := 0
	checkResultTicker := time.NewTicker(time.Duration(TaskCheckInterval) * time.Second)
	defer checkResultTicker.Stop()
	for {
		if count == TaskCheckMaxCount/TaskCheckInterval {
			m.abortMigratingTask(task, ErrTaskTimeout)
			return ErrAbortedMigrateTask
		}
		select {
		case <-checkResultTicker.C:
			count++
			clusterInfo, err := util.ClusterInfoCmd(source.Address)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("ckeck migrate process, cluster info command")
				continue
			}
			if clusterInfo.MigratingSlot != task.MigratingSlot {
				m.abortMigratingTask(task, ErrMismatchMigrateSlot)
				return ErrAbortedMigrateTask
			}
			switch clusterInfo.MigratingState {
			case SlotFail:
				m.abortMigratingTask(task, ErrSlotFailed)
				return ErrAbortedMigrateTask
			case SlotSuccess:
				if err := m.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.MigratingSlot); err != nil {
					m.abortMigratingTask(task, err)
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

func (m *Migrate) hasPendingTasks(namespace, cluster string) bool {
	m.rw.RLock()
	defer m.rw.RUnlock()
	_, ok := m.pendingTasks[util.BuildClusterKey(namespace, cluster)]
	return ok
}

func (m *Migrate) addPendingTasks(namespace, cluster string, tasks []*etcd.MigrateTask) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	if err := m.storage.AddPendingMigrateTask(namespace, cluster, tasks); err != nil {
		return err
	}
	name := util.BuildClusterKey(namespace, cluster)
	m.pendingTasks[name] = append(m.pendingTasks[name], tasks...)
	return nil
}

func (m *Migrate) removePendingTask(namespace, cluster string) *etcd.MigrateTask {
	m.rw.Lock()
	defer m.rw.Unlock()
	name := util.BuildClusterKey(namespace, cluster)
	tasks, ok := m.pendingTasks[name]
	if !ok {
		return nil
	}
	task := tasks[0]
	if err := m.storage.RemovePendingMigrateTask(task); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to remove migrate task from storage")
	}
	if len(tasks) == 1 {
		delete(m.pendingTasks, name)
	} else {
		m.pendingTasks[name] = tasks[1:]
	}
	return task
}

func (m *Migrate) hasMigratingTask(namespace, cluster string) bool {
	m.rw.RLock()
	defer m.rw.RUnlock()
	_, ok := m.migratingTasks[util.BuildClusterKey(namespace, cluster)]
	return ok
}

func (m *Migrate) addMigratingTask(task *etcd.MigrateTask) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	task.Status = TaskStatusMigrating
	task.StartTime = time.Now().Unix()
	if err := m.storage.AddMigrateTask(task); err != nil {
		return err
	}
	m.migratingTasks[util.BuildClusterKey(task.Namespace, task.Cluster)] = task
	return nil
}

func (m *Migrate) removeMigratingTask(task *etcd.MigrateTask) {
	m.rw.Lock()
	defer m.rw.Unlock()
	task.FinishTime = time.Now().Unix()
	delete(m.migratingTasks, util.BuildClusterKey(task.Namespace, task.Cluster))
}

func (m *Migrate) abortMigratingTask(task *etcd.MigrateTask, err error) {
	task.Status = TaskStatusFailed
	task.ErrorDetail = err.Error()
	task.FinishTime = time.Now().Unix()
	_ = m.storage.AddMigrateHistory(task)
	m.removeMigratingTask(task)
	logger.Get().With(
		zap.Error(err),
		zap.Any("task", task),
	).Error("Aborted the migrate task")
}

// finishMigratingTask handler task status and push etcd when task success
func (m *Migrate) finishMigratingTask(task *etcd.MigrateTask) {
	task.Status = TaskStatusSuccess
	_ = m.storage.AddMigrateHistory(task)
	m.removeMigratingTask(task)
	logger.Get().With(
		zap.Any("task", task),
	).Info("Success to migrate the slot")
}
