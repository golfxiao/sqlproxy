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
	"fmt"

	"sqlproxy/core/golog"
	"sqlproxy/mysql"
)

func (c *ClientConn) handleUseDB(dbName string) error {
	if len(dbName) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}
	if c.proxy.GetNode(dbName) == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}
	if !c.CanAccess(dbName) {
		return mysql.NewDefaultError(mysql.ER_DBACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), dbName)
	}

	c.db = dbName
	golog.Debug("ClientConn", "handleUseDB", "switch db", c.connectionId, "db", dbName)
	return c.writeOK(nil)
}
