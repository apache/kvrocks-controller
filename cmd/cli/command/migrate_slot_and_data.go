package command

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var MigrateSlotAndDataCommand = cli.Command{
	Name:      "migrate_slot_and_data",
	Usage:     "Migrate slot and data",
	ArgsUsage: "-s ${source} -t ${target} -S ${slots} -e ${execute}",
	Action:    migrateAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,source",
			Value: -1,
			Usage: "Source shard idx"},
		cli.IntFlag{
			Name:  "t,target",
			Value: -1,
			Usage: "Target shard idx"},
		cli.StringFlag{
			Name:  "S,slots",
			Value: "",
			Usage: `migrate slots, format: single, interval or grouped together by commas
			        eg: 0-4095,8192,10240-16383 `},
		cli.BoolFlag{
			Name:  "e,execute",
			Usage: "Execute the migrate command"},
	},
	Description: `
    Migrate slot and data from source to target under the cluster
    `,
}

func migrateAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Command migrate_slot_and_data should be under cluster dir")
		return
	}

	source := c.Int("s")
	target := c.Int("t")
	slots := c.String("S")
	do := c.Bool("e")
	if source == -1 || target == -1 || len(slots) == 0 {
		fmt.Println("Source shard idx(-s), target shard idx(-t) and migrate slots(-S) must set")
		return
	}

	slotStrs := strings.Split(slots, ",")
	var slotRanges []metadata.SlotRange
	for _, slotStr := range slotStrs {
		slotStr = strings.TrimSpace(slotStr)
		slot, err := metadata.ParseSlotRange(slotStr)
		if err != nil {
			fmt.Println("slot parser error: " + err.Error())
			return
		}
		slotRanges = append(slotRanges, *slot)
	}
	sort.Slice(slotRanges, func(i, j int) bool {
		return slotRanges[i].Start < slotRanges[j].Start
	})

	// new and visualization task
	task := etcd.MigrateTask{
		TaskID:      uint64(time.Now().Unix()),
		SubID:       0,
		Namespace:   ctx.Namespace,
		Cluster:     ctx.Cluster,
		Source:      source,
		Target:      target,
		MigrateSlot: slotRanges,
	}
	if !visableTask(&task) {
		return
	}

	if do {
		var param handlers.MigrateSlotsDataParam
		param.Tasks = append(param.Tasks, &task)
		resp, err := util.HttpPost(handlers.GetMigrateURL(ctx.Leader, ctx.Namespace, ctx.Cluster), param, 5*time.Second)
		responseError("Migrate data", resp, err)
	} else {
		fmt.Println("add -e param to execute the above plan")
	}
}
