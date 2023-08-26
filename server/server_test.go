// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"os"
	"sqlproxy/core/golog"
	"sync"
	"testing"
	"time"

	"sqlproxy/backend"
	"sqlproxy/config"
)

var testServerOnce sync.Once
var testServer *Server
var testDB *backend.BackendProxy
var testConn *ClientConn

var testConfigData = []byte(`
addr : 127.0.0.1:9696
user_list :
- 
    user : testuser
    password : testpwd

nodes :
- 
    name : test
    driver_name: mysql
    datasource: proxy_dml:quanshitestcmnew@tcp(192.168.39.119:3306)/test?charset=utf8mb4&readTimeout=10s&writeTimeout=10s
    max_conns_limit: 5

schema_list :
- 
    user: testuser  
    node: test
`)

type OnConnectListener struct{}

func (this *OnConnectListener) OnConnect(conn *ClientConn) {
	testConn = conn
	golog.Info("OnConnectListener", "OnConnect", "test conn init.", 0)
}

func TestMain(m *testing.M) {
	var err error
	testServer, err = newTestServer()
	if err != nil {
		panic(err)
	}

	// 如果测试backendProxy从testServer中获取连接实例
	// 如果要从外面测sqlproxy服务，则使用newFrontConn来获取连接实例
	// testDB = testServer.GetNode("test")
	testDB = newFrontConn()
	if testDB == nil {
		panic("testDB is nil")
	}

	exitCode := m.Run()

	testServer.Close()

	os.Exit(exitCode)
}

func newTestServer() (*Server, error) {
	cfg, err := config.ParseConfigData(testConfigData)
	if err != nil {
		return nil, err
	}

	testServer, err := NewServer(cfg)
	if err != nil {
		return nil, err
	}
	testServer.acceptListener = &OnConnectListener{}

	go testServer.Run()

	time.Sleep(1 * time.Second)

	return testServer, nil
}

func newFrontConn() *backend.BackendProxy {

	db := backend.NewBackendProxy(config.NodeConfig{
		Name:         "test",
		DriverName:   "mysql",
		Datasource:   "testuser:testpwd@tcp(127.0.0.1:9696)/test?charset=utf8mb4&readTimeout=10s&writeTimeout=10s",
		MaxOpenConns: 2,
	})
	err := db.InitConnectionPool()
	if err != nil {
		panic(err)
	}

	_, err = db.Query("select 1 from dual")
	if err != nil {
		panic(err)
	}

	if testConn == nil {
		panic("testDBConn is nil")
	}

	return db
}

func TestServer(t *testing.T) {
	newTestServer()
}
