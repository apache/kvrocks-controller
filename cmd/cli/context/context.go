package context

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/yaml.v1"
)

const (
	DefaultConfigFile  = "./kc_cli_config.yaml"
	DefaultHistoryFile = "./.kc_cli_history"
)

const (
	LocationRoot = iota
	LocationNamespace
	LocationCluster
)

var DefaultControllers = []string{"127.0.0.1:9379"}

type CliConf struct {
	Controllers []string `yaml:"controllers,omitempty"`
	HistoryFile string   `yaml:"history_file,omitempty"`
}

func LoadConfig(filePath string) (*CliConf, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	conf := &CliConf{}
	if err = yaml.Unmarshal(content, conf); err != nil {
		return nil, err
	}
	return conf, nil
}

type Context struct {
	Leader    string
	Location  int
	Namespace string
	Cluster   string
}

var context *Context
var once sync.Once

func GetContext() *Context {
	once.Do(func() {
		context = &Context{}
	})
	return context
}

func (ctx *Context) ParserLeader(controllers []string) {
	var Err string
	for _, controller := range controllers {
		if resp, err := util.HttpGet(handlers.GetControllerLeaderURL(controller), nil, 5*time.Second); err == nil {
			ctx.Leader = resp.Body.(string)
			if len(ctx.Leader) == 0 {
				fmt.Println("kvrocks controllers no leader, please try again or check controllers" + Err)
				os.Exit(1)
			}
			return
		} else {
			Err = err.Error()
		}
	}
	fmt.Println("Failed to get controller leader, err: " + Err)
	os.Exit(1)
}

func (ctx *Context) EnterNamespace(namespace string) {
	if ctx.Location != LocationRoot {
		fmt.Println("Enter namespace should be under root dir")
		return
	}
	ctx.Namespace = namespace
	ctx.Location = LocationNamespace
}

func (ctx *Context) EnterCluster(cluster string) {
	if ctx.Location != LocationNamespace {
		fmt.Println("Enter cluster should be under namespace dir")
		return
	}
	ctx.Cluster = cluster
	ctx.Location = LocationCluster
}

func (ctx *Context) Outside() {
	switch ctx.Location {
	case LocationNamespace:
		ctx.Location = LocationRoot
		ctx.Namespace = ""
		return
	case LocationCluster:
		ctx.Location = LocationNamespace
		ctx.Cluster = ""
		return
	default:
		return
	}
	return
}
