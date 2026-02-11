package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationTaskStatus_String(t *testing.T) {
	tests := []struct {
		name     string
		status   MigrationTaskStatus
		expected string
	}{
		{"Pending", MigrationTaskPending, "pending"},
		{"Migrating", MigrationTaskMigrating, "migrating"},
		{"Success", MigrationTaskSuccess, "success"},
		{"Failed", MigrationTaskFailed, "failed"},
		{"Unknown", MigrationTaskStatus(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.String())
		})
	}
}

func TestMigrationTask_Marshaling(t *testing.T) {
	now := time.Now().Unix()
	task := &MigrationTask{
		TaskID:         "task-123",
		SubTasks:       []SlotRange{{Start: 0, Stop: 10}},
		SourceShardIdx: 0,
		TargetShardIdx: 1,
		Status:         MigrationTaskPending,
		StartTime:      now,
		SlotOnly:       true,
	}

	data, err := json.Marshal(task)
	require.NoError(t, err)

	var unmarshaledTask MigrationTask
	err = json.Unmarshal(data, &unmarshaledTask)
	require.NoError(t, err)

	assert.Equal(t, task.TaskID, unmarshaledTask.TaskID)
	assert.Equal(t, task.Status, unmarshaledTask.Status)
	assert.Equal(t, task.SlotOnly, unmarshaledTask.SlotOnly)
	assert.Equal(t, task.SubTasks, unmarshaledTask.SubTasks)
}

func TestCluster_IncludeMigrationTasks(t *testing.T) {
	cluster := &Cluster{
		Name: "test-cluster",
		MigrationTasks: []*MigrationTask{
			{
				TaskID: "test-task",
				Status: MigrationTaskMigrating,
			},
		},
	}

	assert.Len(t, cluster.MigrationTasks, 1)
	assert.Equal(t, "test-task", cluster.MigrationTasks[0].TaskID)
	assert.Equal(t, MigrationTaskMigrating, cluster.MigrationTasks[0].Status)

	// Test Clone includes migration tasks
	clonedCluster := cluster.Clone()
	assert.Len(t, clonedCluster.MigrationTasks, 1)
	assert.Equal(t, cluster.MigrationTasks[0].TaskID, clonedCluster.MigrationTasks[0].TaskID)

	// Ensure deep copy
	cluster.MigrationTasks[0].Status = MigrationTaskSuccess
	assert.Equal(t, MigrationTaskMigrating, clonedCluster.MigrationTasks[0].Status)
}
