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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStmt_DropTable(t *testing.T) {
	if _, err := testDB.Exec(`drop table if exists kingshard_test_proxy_stmt`); err != nil {
		t.Fatal(err)
	}
}

func TestStmt_CreateTable(t *testing.T) {
	str := `CREATE TABLE IF NOT EXISTS kingshard_test_proxy_stmt (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256) NOT NULL DEFAULT '',
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	if _, err := testDB.Exec(str); err != nil {
		t.Fatal(err)
	}
}

func TestStmt_Insert(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, str, f, e, u, i) values (?, ?, ?, ?, ?, ?)`

	c := testDB

	pkg, err := c.Exec(str, 1, "a", 3.14, "test1", 255, -127)
	if err != nil {
		t.Fatal(err)
	} else {
		if pkg.AffectedRows != 1 {
			t.Fatal(pkg.AffectedRows)
		}
	}
}

func TestStmt_Select(t *testing.T) {
	str := `select str, f, e from kingshard_test_proxy_stmt where id = ?`

	c := testDB

	result, err := c.Query(str, 1)
	if err != nil {
		t.Fatal(err)
	} else {
		if len(result.Values) != 1 {
			t.Fatal(len(result.Values))
		}

		if len(result.Fields) != 3 {
			t.Fatal(len(result.Fields))
		}

		if str, _ := result.GetString(0, 0); str != "a" {
			t.Fatal("invalid str", str)
		}

		if f, _ := result.GetFloat(0, 1); f != float64(3.14) {
			t.Fatal("invalid f", f)
		}

		if e, _ := result.GetString(0, 2); e != "test1" {
			t.Fatal("invalid e", e)
		}

		if str, _ := result.GetStringByName(0, "str"); str != "a" {
			t.Fatal("invalid str", str)
		}

		if f, _ := result.GetFloatByName(0, "f"); f != float64(3.14) {
			t.Fatal("invalid f", f)
		}

		if e, _ := result.GetStringByName(0, "e"); e != "test1" {
			t.Fatal("invalid e", e)
		}

	}
}

func TestStmt_NULL(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, str, f, e) values (?, ?, ?, ?)`

	c := testDB

	pkg, err := c.Exec(str, 2, nil, 3.14, nil)
	assert.NotNil(t, err)
	assert.Nil(t, pkg)

	pkg, err = c.Exec(str, 2, "", 3.14, nil)
	if err != nil {
		t.Fatal(err)
	} else {
		if pkg.AffectedRows != 1 {
			t.Fatal(pkg.AffectedRows)
		}
	}

	str = `select * from kingshard_test_proxy_stmt where id = ?`
	r, err := c.Query(str, 2)
	if err != nil {
		t.Fatal(err)
	} else {
		if b, err := r.IsNullByName(0, "id"); err != nil {
			t.Fatal(err)
		} else if b == true {
			t.Fatal(b)
		}

		if b, err := r.IsNullByName(0, "str"); err != nil {
			t.Fatal(err)
		} else if b == true {
			t.Fatal(b)
		}

		if b, err := r.IsNullByName(0, "f"); err != nil {
			t.Fatal(err)
		} else if b == true {
			t.Fatal(b)
		}

		if b, err := r.IsNullByName(0, "e"); err != nil {
			t.Fatal(err)
		} else if b == false {
			t.Fatal(b)
		}
	}
}

func TestStmt_Unsigned(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, u) values (?, ?)`

	c := testDB

	pkg, err := c.Exec(str, 3, uint8(255))

	if err != nil {
		t.Fatal(err)
	} else {
		if pkg.AffectedRows != 1 {
			t.Fatal(pkg.AffectedRows)
		}
	}

	str = `select u from kingshard_test_proxy_stmt where id = ?`

	r, err := c.Query(str, 3)
	if err != nil {
		t.Fatal(err)
	} else {
		if u, err := r.GetUint(0, 0); err != nil {
			t.Fatal(err)
		} else if u != uint64(255) {
			t.Fatal(u)
		}
	}
}

func TestStmt_Signed(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, i) values (?, ?)`

	c := testDB

	if _, err := c.Exec(str, 4, 127); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Exec(str, uint64(18446744073709551516), int8(-128)); err != nil {
		t.Fatal(err)
	}

}

func TestStmt_NotNullInsert(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, str, f, e, u, i) values (?, ?, ?, ?, ?, ?)`

	c := testDB

	// 测试给not null 字段插入空串
	pkg, err := c.Exec(str, 1000, "", 3.14, "test1", 255, -127)
	if err != nil {
		t.Fatal(err)
	} else {
		if pkg.AffectedRows != 1 {
			t.Fatal(pkg.AffectedRows)
		}
	}
}

func TestStmt_Trans(t *testing.T) {
	c := testDB

	if _, err := c.Exec(`insert into kingshard_test_proxy_stmt (id, str) values (1002, "abc")`); err != nil {
		t.Fatal(err)
	}

	c1, err := c.Begin()
	if err != nil {
		t.Fatal(err)
	}

	str := `select str from kingshard_test_proxy_stmt where id = ?`

	if _, err := c1.Query(str, 1002); err != nil {
		t.Fatal(err)
	}

	if err := c1.Commit(); err != nil {
		t.Fatal(err)
	}

	if r, err := c.Query(str, 1002); err != nil {
		t.Fatal(err)
	} else {
		if str, _ := r.GetString(0, 0); str != `abc` {
			t.Fatal(str)
		}
	}
}
