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
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"sync"

	"sqlproxy/backend"
	"sqlproxy/core/golog"
	"sqlproxy/core/hack"
	"sqlproxy/mysql"
)

// client <-> proxy
type ClientConn struct {
	sync.Mutex

	pkg *mysql.PacketIO

	c net.Conn

	proxy *Server

	capability uint32

	connectionId uint32

	status    uint16
	collation mysql.CollationId
	charset   string

	user string
	db   string

	salt []byte

	txConn *backend.BackendProxy

	closed bool

	lastInsertId int64
	affectedRows int64

	stmtId uint32

	stmts map[uint32]*Stmt //prepare相关,client端到proxy的stmt

	configVer uint32 //check config version for reload online
}

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

var baseConnId uint32 = 10000

func (c *ClientConn) CanAccess(db string) bool {
	nodes, _ := c.proxy.schemas[c.user]
	if len(nodes) == 0 {
		return true
	}
	return StrInSlice(db, nodes)
}

// GetBackendDB returns the backend database of the ClientConn.
//
// It checks if the transaction connection (txConn) is not nil and returns it.
// If the transaction connection is nil, it checks if the backend schema for the user exists and returns it.
// If the backend schema does not exist, it checks if the backend node for the database exists and returns it.
// If neither the backend schema nor the backend node exists, it returns nil.
//
// Returns a pointer to backend.BackendProxy.
func (c *ClientConn) GetBackendDB() *backend.BackendProxy {
	if c.txConn != nil {
		return c.txConn
	}
	if backend := c.proxy.GetNode(c.db); backend != nil {
		return backend
	}
	return nil
}

func (c *ClientConn) IsAllowConnect() bool {
	clientHost, _, err := net.SplitHostPort(c.c.RemoteAddr().String())
	if err != nil {
		fmt.Println(err)
	}
	clientIP := net.ParseIP(clientHost)

	current, _, _ := c.proxy.allowipsIndex.Get()
	ipVec := c.proxy.allowips[current]
	if ipVecLen := len(ipVec); ipVecLen == 0 {
		return true
	}
	for _, ip := range ipVec {
		if ip.Match(clientIP) {
			return true
		}
	}

	golog.Error("server", "IsAllowConnect", "error", mysql.ER_ACCESS_DENIED_ERROR,
		"ip address", c.c.RemoteAddr().String(), " access denied by kindshard.")
	return false
}

func (c *ClientConn) Handshake() error {
	if err := c.writeInitialHandshake(); err != nil {
		golog.Error("server", "Handshake", err.Error(),
			c.connectionId, "msg", "send initial handshake error")
		return err
	}

	if err := c.readHandshakeResponse(); err != nil {
		golog.Error("server", "readHandshakeResponse",
			err.Error(), c.connectionId,
			"msg", "read Handshake Response error")
		return err
	}

	if err := c.writeOK(nil); err != nil {
		golog.Error("server", "readHandshakeResponse",
			"write ok fail",
			c.connectionId, "error", err.Error())
		return err
	}

	c.pkg.Sequence = 0
	return nil
}

func (c *ClientConn) Close() error {
	if c.closed {
		return nil
	}

	c.c.Close()

	c.closed = true

	golog.Info("server", "Close", "", c.connectionId)
	return nil
}

func (c *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return c.writePacket(data)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return c.pkg.WritePacketBatch(total, data, direct)
}

func (c *ClientConn) readHandshakeResponse() error {
	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	//check user
	if _, ok := c.proxy.users[c.user]; !ok {
		golog.Error("ClientConn", "readHandshakeResponse", "user error", 0,
			"auth", auth,
			"client_user", c.user,
			"config_set_user", c.user,
			"password", c.proxy.users[c.user])
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	//check password
	checkAuth := mysql.CalcPassword(c.salt, []byte(c.proxy.users[c.user]))
	if !bytes.Equal(auth, checkAuth) {
		golog.Error("ClientConn", "readHandshakeResponse", "password error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"user", c.user,
			"salt", c.salt,
			"password", c.proxy.users[c.user])
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	pos += authLen

	var db string
	if c.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(c.db) + 1
	}
	if db != "" && !c.CanAccess(db) {
		golog.Error("ClientConn", "readHandshakeResponse", "db access error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"client_user", c.user,
			"db", db)
		return mysql.NewDefaultError(mysql.ER_DBACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), db)
	}

	c.db = db
	golog.Debug("ClientConn", "readHandshakeResponse", "init db", c.connectionId, "db", db)
	return nil
}

func (c *ClientConn) clean() {
	golog.Info("ClientConn", "clean", "", c.connectionId)
	if c.txConn != nil {
		c.txConn.Commit() // TODO check possible problems?
		c.txConn = nil
	}
}

func (c *ClientConn) Run() {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			golog.Error("ClientConn", "Run",
				err.Error(), c.connectionId,
				"stack", string(buf))
		}

		c.Close()
	}()
	defer c.clean()
	for {
		data, err := c.readPacket()
		if err != nil {
			golog.Error("ClientConn", "Run", err.Error(), c.connectionId)
			return
		}

		if c.configVer != c.proxy.configVer {
			err := c.reloadConfig()
			if nil != err {
				golog.Error("ClientConn", "Run",
					err.Error(), c.connectionId,
				)
				c.writeError(err)
				return
			}
			c.configVer = c.proxy.configVer
			golog.Debug("ClientConn", "Run",
				fmt.Sprintf("config reload ok, ver:%d", c.configVer), c.connectionId,
			)
		}

		if err := c.dispatch(data); err != nil {
			c.proxy.counter.IncrErrLogTotal()
			golog.Error("ClientConn", "Run",
				err.Error(), c.connectionId,
			)
			c.writeError(err)
			if err == mysql.ErrBadConn {
				c.Close()
			}
		}

		if c.closed {
			return
		}

		c.pkg.Sequence = 0
	}
}

func (c *ClientConn) dispatch(data []byte) error {
	c.proxy.counter.IncrClientQPS()
	cmd := data[0]
	data = data[1:]

	golog.Debug("ClientConn", "dispatch", "receive cmd", c.connectionId, "cmd", mysql.COM_TOKEN_MAP[cmd])

	switch cmd {
	case mysql.COM_QUIT:
		return c.handleQuit()
	case mysql.COM_QUERY:
		return c.handleQuery(hack.String(data))
	case mysql.COM_PING:
		return c.handlePing()
	case mysql.COM_INIT_DB:
		return c.handleInitDB(hack.String(data))
	// case mysql.COM_FIELD_LIST:
	// 	return c.handleFieldList(data)
	case mysql.COM_STMT_PREPARE:
		return c.handleStmtPrepare(hack.String(data))
	case mysql.COM_STMT_EXECUTE:
		return c.handleStmtExecute(data)
	case mysql.COM_STMT_CLOSE:
		return c.handleStmtClose(data)
	case mysql.COM_STMT_SEND_LONG_DATA:
		return c.handleStmtSendLongData(data)
	case mysql.COM_STMT_RESET:
		return c.handleStmtReset(data)
	case mysql.COM_SET_OPTION:
		return c.writeEOF(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		golog.Error("ClientConn", "dispatch", msg, c.connectionId)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}
}

func (c *ClientConn) handlePing() error {
	return c.writeOK(nil)
}

func (c *ClientConn) handleInitDB(dbName string) error {
	return c.handleUseDB(dbName)
}

func (c *ClientConn) handleQuit() error {
	c.handleRollback()
	c.Close()
	return nil
}

func (c *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	golog.Debug("ClientConn", "writeOK", "result info", c.connectionId,
		"status", r.Status, "affectedRows", r.AffectedRows, "insertId", r.InsertId)
	return c.writePacket(data)
}

func (c *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacketBatch(total, data, direct)
}

func (c *ClientConn) reloadConfig() error {
	c.proxy.configUpdateMutex.RLock()
	defer c.proxy.configUpdateMutex.RUnlock()
	if _, ok := c.proxy.users[c.user]; !ok {
		return fmt.Errorf("user [%s] is null or user is deleted", c.user)
	}
	if !c.CanAccess(c.db) {
		return fmt.Errorf("db [%s] access denied", c.db)
	}

	return nil
}
