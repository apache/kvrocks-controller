package command

import (
	"fmt"
	"time"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var MigrateShowCommand = cli.Command{
	Name:      "showmig",
	Usage:     "show migrate tasks",
	ShortName: "showm",
	ArgsUsage: "doing | pending | history",
	Action:    migrateShowAction,
	Description: `
    show migrate tasks
    `,
}

type MigrateTask struct {
	TaskID 		uint64 `json:"taskid"` // multi tasks allow have the same taskid
	SubID 		uint64 `json:"subid"`  // different subid under the same taskid
	Namespace   string `json:"namespace"`
	Cluster     string `json:"cluster"`
   	Source      int    `json:"source"` // soucre shardIdx
   	Target      int    `json:"target"` // target shardIdx
   	MigrateSlot []metadata.SlotRange `json:"migrate_slots"` // migrate slots
   	SlotDoing   int    `json:"doing_slot"`

   	// statistics
   	PendingTime int64   `json:"pending_time"`
   	DoingTime   int64   `json:"doing_time"` 
   	DoneTime    int64   `json:"done_time"`

   	Status      int     `json:"status"` // init,penging,doing,success/failed
   	Err         string  `json:"error"` // if failed, Err is not nil
}

var (
	showMigItems = []string{"TaskID", "SubID", "Source", "Target", "Slots", 
					"Status", "Err", "Peding", "Doing", "Done"}
)

type MigTask struct {
	TaskID uint64
	SubID  uint64
	Source int
	Target int
   	Slots  string
   	Status int
   	Err    string
	Peding string   
   	Doing  string   
   	Done   string
}

func migrateShowAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("failover command should under clsuter dir")
		return 
	}

	if len(c.Args()) < 1 || (c.Args()[0] != "history" && c.Args()[0] != "pending" && c.Args()[0] != "doing") {
		fmt.Println("should set param 'doing | pending | history'")
		return 
	}

	qtype := c.Args()[0]
	resp, err := util.HttpGet(handlers.GetClusterMigrateURL(ctx.Leader, ctx.Namespace, ctx.Cluster, qtype), nil, 5 * time.Second)
	if HttpResponeException("failover node", resp, err) {
		return
	}

	var tasks []*etcd.MigrateTask
	err = util.InterfaceToStruct(resp.Body, &tasks)
	if err != nil {
		fmt.Println("response transfer struct error: ", err)
		return
	}

	var showTasks []*MigTask
	for idx, task := range tasks {
		stask := &MigTask{
			TaskID: task.TaskID,
			SubID:  task.SubID,
			Source: task.Source,
			Target: task.Target,
		   	Status: task.Status,
		   	Err:    task.Err,
			Peding: time.Unix(task.PendingTime, 0).Format("2006-01-02 15:04:05"),   
		   	Doing:  time.Unix(task.DoingTime, 0).Format("2006-01-02 15:04:05"),
		   	Done:   time.Unix(task.DoneTime, 0).Format("2006-01-02 15:04:05"), 
		}
		for _, slots := range task.MigrateSlot {
			stask.Slots = stask.Slots + " " + slots.String()
		}
		if len(stask.Err) == 0 {
			stask.Err = "nil"
		}
		if idx > 0 && tasks[idx - 1].TaskID != task.TaskID {
			showTasks = append(showTasks, nil)
		}
		showTasks = append(showTasks, stask)
	}
	showTasks = append(showTasks, nil)
	showTasks = showTasks[0: len(showTasks)-1]

	util.PrintTable(showMigItems, migratetasksToInterfaceSlice(showTasks))
	return 
}