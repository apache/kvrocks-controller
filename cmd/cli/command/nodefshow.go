package command

import (
	"fmt"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var FailoverShowCommand = cli.Command{
	Name:      "showfailover",
	ShortName: "showf",
	Usage:     "show failover tasks",
	ArgsUsage: "pending | history",
	Action:    failoverShowAction,
	Description: `
    show failover tasks
    `,
}

var (
	showFailItems = []string{"ShardID", "Addr", "Role", "Status", "Err", "Peding", "Doing", "Done"}
)

type FailTask struct {
	ShardID int
	Addr    string
	Role    string
	Status  int
	Err     string
	Peding  string
	Doing   string
	Done    string
}

func failoverShowAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("failover command should under clsuter dir")
		return
	}

	if len(c.Args()) < 1 || (c.Args()[0] != "history" && c.Args()[0] != "pending") {
		fmt.Println("should set param 'history | pending'")
		return
	}

	qtype := c.Args()[0]
	resp, err := util.HttpGet(handlers.GetClusterFailoverURL(ctx.Leader, ctx.Namespace, ctx.Cluster, qtype), nil, 5*time.Second)
	if HttpResponeException("failover node", resp, err) {
		return
	}

	var tasks []*etcd.FailoverTask
	err = util.InterfaceToStruct(resp.Body, &tasks)
	if err != nil {
		fmt.Println("response transfer struct error: ", err)
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
			Peding:  time.Unix(task.PendingTime, 0).Format("2006-01-02 15:04:05"),
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
