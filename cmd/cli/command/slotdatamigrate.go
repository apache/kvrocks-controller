package command

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var MigrateCommand = cli.Command{
	Name:      "migdata",
	Usage:     "migrate slots and data",
	ArgsUsage: "-s ${sourceIdx} -t ${targetIdx} -l ${slotrange} -d",
	Action:    migrateAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,sourceIdx",
			Value: -1,
			Usage: "source shard idx"},
		cli.IntFlag{
			Name:  "t,targetIdx",
			Value: -1,
			Usage: "target shard idx"},
		cli.StringFlag{
			Name:  "l,slots",
			Value: "",
			Usage: `migrate slots, format: single, interval or grouped together by commas
			        eg: 0-4095,8192,10240-16383 `},
		cli.BoolFlag{
			Name:  "d,do",
			Usage: "flag do migrate task"},
	},
	Description: `
    migrate slots data from source shard to target shard under special cluster
    `,
}

func migrateAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("migrate command should under cluster dir")
		return
	}

	source := c.Int("s")
	target := c.Int("t")
	slots := c.String("l")
	do := c.Bool("d")
	if source == -1 || target == -1 || len(slots) == 0 {
		fmt.Println("source shard idx(-s), target shard idx(-t) and migrate slots(-l) must set")
		return
	}

	// parser and sort slotrange
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
		HttpResponeException("migrate data", resp, err)
	} else {
		fmt.Println("add -d param, do above migrate task plan")
	}
	return
}
