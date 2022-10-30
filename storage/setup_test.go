package storage

import (
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var testEtcdClient *clientv3.Client

func setup() (err error) {
	testEtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"0.0.0.0:2379"},
		DialTimeout: 5 * time.Second,
	})
	return err
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		panic("Failed to setup the etcd client: " + err.Error())
	}
	os.Exit(m.Run())
}
