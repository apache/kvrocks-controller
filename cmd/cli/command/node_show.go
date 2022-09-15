package command

import (
	"fmt"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var ShowFailoverTasksCommand = cli.Command{
	Name:      "show_failover_tasks",
	Usage:     "Show failover tasks",
	ArgsUsage: "pending | history",
	Action:    failoverShowAction,
	Description: `
    Show failover tasks
    `,
}

var (
	showFailItems = []string{"ShardID", "Addr", "Role", "Status", "Err", "Pending", "Doing", "Done"}
)

type FailTask struct {
	ShardID int
	Addr    string
	Role    string
	Status  int
	Err     string
	Pending string
	Doing   string
	Done    string
}

func failoverShowAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Command command should be under the cluster dir")
		return
	}

	if len(c.Args()) < 1 || (c.Args()[0] != "history" && c.Args()[0] != "pending") {
		fmt.Println("Param should be 'history | pending'")
		return
	}

	qtype := c.Args()[0]
	resp, err := util.HttpGet(handlers.GetClusterFailoverURL(ctx.Leader, ctx.Namespace, ctx.Cluster, qtype), nil, 5*time.Second)
	if responseError("Failover node", resp, err) {
		return
	}

	var tasks []*etcd.FailOverTask
	err = util.InterfaceToStruct(resp.Data, &tasks)
	if err != nil {
		fmt.Println("Internal error: ", err)
		return
	}

	var showTasks []*FailTask
	for _, task := range tasks {
		stask := &FailTask{
			ShardID: task.ShardIdx,
			Addr:    task.Node.Address,
			Role:    task.Node.Role,
			Status:  task.Status,
			Err:     task.Err,
			Pending: time.Unix(task.PendingTime, 0).Format("2006-01-02 15:04:05"),
			Doing:   time.Unix(task.DoingTime, 0).Format("2006-01-02 15:04:05"),
			Done:    time.Unix(task.DoneTime, 0).Format("2006-01-02 15:04:05"),
		}
		if len(stask.Err) == 0 {
			stask.Err = "nil"
		}
		showTasks = append(showTasks, stask)
	}
	showTasks = append(showTasks, nil)
	showTasks = showTasks[0 : len(showTasks)-1]

	util.PrintTable(showFailItems, failtasksToInterfaceSlice(showTasks))
	return
}
