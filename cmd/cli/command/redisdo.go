package command

import (
	"fmt"
	"context"

	"gopkg.in/urfave/cli.v1"
	clictx "github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
)

var RedisDoCommand = cli.Command{
	Name:      "do",
	Usage:     "do redis command to node",
	ArgsUsage: "${node_addr} ${redis_command} ${args}...",
	Action:    doAction,
	Description: `
    send redis command to node
    `,
}

func doAction(c *cli.Context) {
    args := clictx.GetContext().ReidsArgs
    if len(args) < 2 {
    	fmt.Println("do command at least 2 params")
    	return 
    }
    node := args[0]
    args = args[1:]
    var redisArgs []interface{}
    for _, arg := range args {
    	redisArgs = append(redisArgs, arg)
    }

    client, err := util.RedisPool(node)
	if err != nil {
		fmt.Println("addr: %s, dail error : %w", node, err)
		return
	}
    res, err := client.Do(context.Background(), redisArgs...).Result()
    if err != nil {
		fmt.Println("do error: ", err)
		return 
	}
	fmt.Println(res.(string))
	return 
}