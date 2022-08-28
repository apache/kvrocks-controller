package main

import (
	"fmt"
	"os"
	"strings"

	linenoise "github.com/GeertJohan/go.linenoise"
	c "github.com/KvrocksLabs/kvrocks_controller/cmd/cli/command"
	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var rootCommands = []cli.Command{
	c.ListCommand,
	c.CdCommand,
	c.RmCommand,
}

var namespaceCommands = []cli.Command{
	c.CreateNamespaceCommand,
}

var clusterCommands = []cli.Command{
	c.CreateClusterCommand,
	c.ShowClusterCommand,
	c.ShowFailoverTasksCommand,
	c.MigrateShowCommand,
	c.SyncClusterTopoCommand,
	c.SendCommandToClusterCommand,
}

var shardCommands = []cli.Command{
	c.AddShardCommand,
	c.DelShardCommand,
	c.MigrateSlotAndDataCommand,
	c.MigrateSlotsCommand,
}

var nodeCommands = []cli.Command{
	c.AddNodeCommand,
	c.DelNodeCommand,
	c.FailoverCommand,
	c.SyncTopoToNodeCommand,
	c.SendRedisCommandToNodeCommand,
}

var commands = map[string]cli.Command{}
var ctx *context.Context

func init() {
	for _, cmd := range rootCommands {
		commands[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			commands[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range namespaceCommands {
		commands[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			commands[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range clusterCommands {
		commands[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			commands[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range shardCommands {
		commands[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			commands[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range nodeCommands {
		commands[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			commands[cmd.ShortName] = cmd
		}
	}
}

func getDir() string {
	switch ctx.Location {
	case context.LocationNamespace:
		return "/" + ctx.Namespace + "/> "
	case context.LocationCluster:
		return "/" + ctx.Namespace + "/" + ctx.Cluster + "/> "
	default:
		return "/>"
	}
}

func showHelp() {
	fmt.Println("List of root commands:")
	for _, cmd := range rootCommands {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of namespace commands:")
	for _, cmd := range namespaceCommands {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of cluster commands:")
	for _, cmd := range clusterCommands {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of shard commands:")
	for _, cmd := range shardCommands {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of node commands:")
	for _, cmd := range nodeCommands {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
}

func main() {
	var (
		conf *context.CliConf
		err  error
	)
	if len(os.Args) == 3 && string(os.Args[1]) == "--peers" {
		conf = &context.CliConf{
			Controllers: strings.Split(os.Args[2], ","),
			HistoryFile: context.DefaultHistoryFile,
		}
	} else if len(os.Args) == 3 && string(os.Args[1]) == "--config" {
		conf, err = context.LoadConfig(os.Args[2])
	} else {
		conf = &context.CliConf{
			Controllers: context.DefaultControllers,
			HistoryFile: context.DefaultHistoryFile,
		}
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// show help
	if len(os.Args) == 2 && (string(os.Args[1]) == "-h" || string(os.Args[1]) == "--help") {
		help := `Usage:
		cli is interactive kvrocks controller devops tool
		./kvrocks-controller-cli enter interactive mode, help subcommand show usage
		--peers ${controller-1-addr, controller-2-addr, ...} set controllers list
	 	--config ${config} config file path
		`
		fmt.Println(help)
		os.Exit(0)
	}
	ctx = context.GetContext()
	ctx.ParserLeader(conf.Controllers)
	if conf.HistoryFile == "" {
		conf.HistoryFile = context.DefaultHistoryFile
	}
	_, err = os.Stat(conf.HistoryFile)
	if err != nil && os.IsNotExist(err) {
		_, err = os.Create(conf.HistoryFile)
		if err != nil {
			fmt.Println(conf.HistoryFile + " create failed")
		}
	}

	err = linenoise.LoadHistory(conf.HistoryFile)
	if err != nil {
		fmt.Println(err)
	}
	defer util.CloseRedisClients()
	for {
		str, err := linenoise.Line(getDir())
		if err != nil {
			if err == linenoise.KillSignalError {
				os.Exit(1)
			}
			fmt.Printf("Unexpected error: %s\n", err)
			os.Exit(1)
		}
		fields := strings.Fields(str)

		linenoise.AddHistory(str)
		err = linenoise.SaveHistory(conf.HistoryFile)
		if err != nil {
			fmt.Println(err)
		}

		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "help":
			showHelp()
			continue
		case "quit":
			os.Exit(0)
		}

		cmd, ok := commands[fields[0]]
		if !ok {
			fmt.Println("Error: unknown command.")
			continue
		}
		app := cli.NewApp()
		app.Name = cmd.Name
		app.Commands = []cli.Command{cmd}
		app.Run(append(os.Args[:1], fields...))
	}
}
