package command

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var MigrateSlotsCommand = cli.Command{
	Name:      "migrate_slot_only",
	Usage:     "Migrate slots only NOT include data",
	ArgsUsage: "-s ${source} -t ${target} -S ${slots}",
	Action:    migrateSlotsAction,
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
			Usage: `Migrate slots, format: single, interval or grouped together by commas
			        eg: 0-4095,8192,10240-16383 `},
	},
	Description: `
    Migrate slots from source shard to target shard under special cluster
    `,
}

func migrateSlotsAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Command migrate_slot_only should under cluster dir")
		return
	}

	source := c.Int("s")
	target := c.Int("t")
	slots := c.String("S")
	if source == -1 || target == -1 || len(slots) == 0 {
		fmt.Println("source shard idx(-s), target shard idx(-t) and migrate slots(-S) must set")
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

	param := &handlers.MigrateSlotsParam{
		SourceShardIdx: source,
		TargetShardIdx: target,
		SlotRanges:     slotRanges,
	}
	resp, err := util.HttpPost(handlers.GetMigrateSlotsURL(ctx.Leader, ctx.Namespace, ctx.Cluster), param, 5*time.Second)
	HttpResponeException("Migrate slots", resp, err)
}
