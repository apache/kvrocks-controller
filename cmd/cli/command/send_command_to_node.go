package command

import (
	"context"
	"fmt"

	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var SendRedisCommandToNodeCommand = cli.Command{
	Name:      "send_command_to_node",
	Usage:     "Send redis command to the node",
	ArgsUsage: "${node_addr} ${redis_command} ${args}...",
	Action:    sendRedisCommandToNode,
	Description: `
    send redis command to node
    `,
}

func sendRedisCommandToNode(c *cli.Context) {
	if len(c.Args()) < 2 {
		fmt.Println("Required at least 2 params")
		return
	}
	node := c.Args()[0]
	args := c.Args()[1:]
	var redisArgs []interface{}
	for _, arg := range args {
		redisArgs = append(redisArgs, arg)
	}

	client, err := util.NewRedisClient(node)
	if err != nil {
		fmt.Printf("addr: %s, dail error : %s\n", node, err.Error())
		return
	}
	res, err := client.Do(context.Background(), redisArgs...).Result()
	if err != nil {
		fmt.Println("do error: ", err)
		return
	}
	fmt.Println(res)
	return
}
