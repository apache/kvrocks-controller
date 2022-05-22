package command

import (
	"fmt"
	"time"
	"reflect"
	"strings"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var ListCommand = cli.Command{
	Name:      "ls",
	Usage:     "list namespace or cluster",
	Action:    listAction,
	Description: `
    ls namespaces or ls clusters under special namespace
    `,
}

func getNames(names interface{}) []string {
	var res []string
	objs := reflect.ValueOf(names)
	for i := 0; i < objs.Len(); i++ {
		obj := objs.Index(i)
		res = append(res, obj.Interface().(string))
	}
	return res
}

func showList(names []string, title string) {
	fmt.Println(title + ":")	
	for _, name :=range names {
		fmt.Printf("\t%s\n", name)
	}
}

func listAction(c *cli.Context) {
	ctx := context.GetContext()
	switch ctx.Location {
	case context.LocationRoot:
		resp, err := util.HttpGet(handlers.GetNamespaceRootURL(ctx.Leader), nil, 5 * time.Second)
		if err != nil {
			fmt.Println("list namespcae error: " + err.Error())
			return 
		}
		if resp.Errno != handlers.Success {
			fmt.Println("list namespcae error: " + resp.Errmsg)	
			return
		}
		if resp.Body == nil {
			fmt.Println("no namespace")
			return
		}
		showList(getNames(resp.Body), "namespace")
	case context.LocationNamespace:
		resp, err := util.HttpGet(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), nil, 5 * time.Second)
		if err != nil {
			fmt.Println("list cluster error: " + err.Error())
			return 
		}
		if resp.Errno != handlers.Success {
			fmt.Println("list cluster error: " + resp.Errmsg)	
			return
		}
		if resp.Body == nil {
			fmt.Println("no cluster")
			return
		}
		showList(getNames(resp.Body), "cluster")
	default:
		return 
	}
	return 
}

var CdCommand = cli.Command{
	Name:      "cd",
	Usage:     "cd namespcae or cd cluster",
	ArgsUsage: "cd ${namespace} or cd ${clsuter}",
	Action:    cdAction,
	Description: `
    cd special namespaces or special cluster
    `,
}

func cdAction(c *cli.Context) {
	ctx := context.GetContext()
	name := ctx.CdName
	if len(name) == 0 {
		return
	}

	if name == ".." {
		ctx.Outside()
		return
	}

	switch ctx.Location {
	case context.LocationRoot:
		resp, err := util.HttpGet(handlers.GetNamespaceRootURL(ctx.Leader), nil, 5 * time.Second)
		if err != nil {
			fmt.Println("list namespcae error: " + err.Error())
			return 
		}
		if resp.Errno != handlers.Success {
			fmt.Println("list namespcae error: " + resp.Errmsg)	
			return
		}
		if resp.Body == nil {
			fmt.Println("no namespace")
			return
		}
		namespaces := getNames(resp.Body)
		for _, namespace :=range namespaces {
			if name == namespace {
				ctx.EnterNamespace(name)
				return
			}
		}
		return 
	case context.LocationNamespace:
		resp, err := util.HttpGet(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), nil, 5 * time.Second)
		if err != nil {
			fmt.Println("list cluster error: " + err.Error())
			return 
		}
		if resp.Errno != handlers.Success {
			fmt.Println("list cluster error: " + resp.Errmsg)	
			return
		}
		if resp.Body == nil {
			fmt.Println("no cluster")
			return
		}
		clusters := getNames(resp.Body)
		for _, cluster :=range clusters {
			if name == cluster {
				ctx.EnterCluster(name)
				return
			}
		}
		return 
	}
	return 
}

var RmCommand = cli.Command{
	Name:   "rm",
	Usage:  "rm current namespaces or rm current cluster",
	Action: rmAction,
	Description: `
    rm current namespaces or current cluster
    if rm namespaces, the namespaces must not contain clusters
    `,
}

func rmAction(c *cli.Context) {
	ctx := context.GetContext()
	switch ctx.Location {
	case context.LocationNamespace:
		resp, err := util.HttpDelete(handlers.GetNamespaceURL(ctx.Leader, ctx.Namespace), nil, 5 * time.Second)
		if err != nil {
			fmt.Println("rm namespcae error: " + err.Error())
			return 
		}
		if resp.Errno != handlers.Success {
			fmt.Println("rm namespcae error: " + resp.Errmsg)	
			return
		}
		if resp.Body == nil {
			fmt.Println("rm namespcae error")
			return
		}
		fmt.Println(resp.Body.(string))
	case context.LocationCluster:
		resp, err := util.HttpDelete(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 5 * time.Second)
		if err != nil {
			fmt.Println("rm cluster error: " + err.Error())
			return 
		}
		if resp.Errno != handlers.Success {
			fmt.Println("rm cluster error: " + resp.Errmsg)	
			return
		}
		if resp.Body == nil {
			fmt.Println("rm cluster error")
			return
		}
		fmt.Println(resp.Body.(string))
	default:
		return 
	}
	ctx.Outside()
	return 
}

var MakeNsCommand = cli.Command{
	Name:      "mkns",
	Usage:     "make namespcae",
	ArgsUsage: "mkns ${namespace}",
	Action:    mknsAction,
	Description: `
    create namespce
    `,
}

func mknsAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationRoot {
		fmt.Println("mkns need return rootlocation '/'")
		return 
	}
	if len(ctx.MknsName) == 0 {
		fmt.Println("mkns need set namespcae")
		return 
	}
	if strings.Contains(ctx.MknsName, "/") {
		fmt.Println("namespcae can't contain '/'")
		return 
	}
	name := ctx.MknsName
	resp, err := util.HttpPost(handlers.GetNamespaceRootURL(ctx.Leader), handlers.CreateNamespaceParam{Namespace: name,}, 5 * time.Second)
	if err != nil {
		fmt.Println("make namespcae error: " + err.Error())
		return 
	}
	if resp.Errno != handlers.Success {
		fmt.Println("make namespcae error: " + resp.Errmsg)	
		return
	}
	if resp.Body == nil {
		fmt.Println("make namespcae error")
		return
	}
	ctx.EnterNamespace(name)
	fmt.Println(resp.Body.(string))
}