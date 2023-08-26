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
	"sqlproxy/core/golog"
	"sqlproxy/mysql"
)

func (c *ClientConn) isInTransaction() bool {
	return c.status&mysql.SERVER_STATUS_IN_TRANS > 0 ||
		!c.isAutoCommit()
}

func (c *ClientConn) isAutoCommit() bool {
	return c.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *ClientConn) handleBegin() error {
	// for _, co := range c.txConns {
	// 	if err := co.Begin(); err != nil {
	// 		return err
	// 	}
	// }
	// 异常处理，前一个事务未释放，又开启一个新事务，需要先把前一个事务提交
	if c.txConn != nil {
		golog.Info("ClientConn", "handleBegin", "txConn is not nil, try to commit first.", c.connectionId)
		if err := c.txConn.Commit(); err != nil {
			golog.Warn("ClientConn", "handleBegin", err.Error(), c.connectionId)
		}
	}
	backend := c.GetBackendDB()
	if backend == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	txConn, err := backend.Begin()
	if err != nil {
		return err
	}
	c.txConn = txConn
	c.status |= mysql.SERVER_STATUS_IN_TRANS
	return c.writeOK(nil)
}

func (c *ClientConn) handleCommit() (err error) {
	if err := c.commit(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *ClientConn) handleRollback() (err error) {
	if err := c.rollback(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *ClientConn) commit() (err error) {
	c.status &= ^mysql.SERVER_STATUS_IN_TRANS

	if c.txConn == nil {
		golog.Warn("ClientConn", "commit", "txConn is nil", c.connectionId)
		return
	}
	if err = c.txConn.Commit(); err != nil {
		return err
	}
	c.txConn = nil
	return
}

func (c *ClientConn) rollback() (err error) {
	c.status &= ^mysql.SERVER_STATUS_IN_TRANS

	if c.txConn == nil {
		golog.Warn("ClientConn", "rollback", "txConn is nil", c.connectionId)
		return
	}
	if err = c.txConn.Rollback(); err != nil {
		return err
	}
	c.txConn = nil
	return
}
