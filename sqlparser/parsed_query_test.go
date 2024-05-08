/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"reflect"
	"testing"

	"sqlproxy/sqlparser/dependency/sqltypes"

	"sqlproxy/sqlparser/dependency/querypb"
)

func TestNewParsedQuery(t *testing.T) {
	stmt, err := Parse("select * from a where id =:id")
	if err != nil {
		t.Error(err)
		return
	}
	pq := NewParsedQuery(stmt)
	want := &ParsedQuery{
		Query:         "select * from a where id = :id",
		bindLocations: []bindLocation{{offset: 27, length: 3}},
	}
	if !reflect.DeepEqual(pq, want) {
		t.Errorf("GenerateParsedQuery: %+v, want %+v", pq, want)
	}
}

func TestGenerateQuery(t *testing.T) {
	tcases := []struct {
		desc     string
		query    string
		bindVars map[string]*querypb.BindVariable
		extras   map[string]Encodable
		output   string
	}{
		{
			desc:  "no substitutions",
			query: "select * from a where id = 2",
			bindVars: map[string]*querypb.BindVariable{
				"id": sqltypes.Int64BindVariable(1),
			},
			output: "select * from a where id = 2",
		}, {
			desc:  "missing bind var",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*querypb.BindVariable{
				"id1": sqltypes.Int64BindVariable(1),
			},
			output: "missing bind var id2",
		}, {
			desc:  "simple bindvar substitution",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*querypb.BindVariable{
				"id1": sqltypes.Int64BindVariable(1),
				"id2": sqltypes.NullBindVariable,
			},
			output: "select * from a where id1 = 1 and id2 = null",
		}, {
			desc:  "tuple *querypb.BindVariable",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.TestBindVariable([]interface{}{1, "aa"}),
			},
			output: "select * from a where id in (1, 'aa')",
		}, {
			desc:  "list bind vars 0 arguments",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.TestBindVariable([]interface{}{}),
			},
			output: "empty list supplied for vals",
		}, {
			desc:  "non-list bind var supplied",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.Int64BindVariable(1),
			},
			output: "unexpected list arg type (INT64) for key vals",
		}, {
			desc:  "list bind var for non-list",
			query: "select * from a where id = :vals",
			bindVars: map[string]*querypb.BindVariable{
				"vals": sqltypes.TestBindVariable([]interface{}{1}),
			},
			output: "unexpected arg type (TUPLE) for non-list key vals",
		}, {
			desc:  "single column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []ColIdent{NewColIdent("pk")},
					Rows: [][]sqltypes.Value{
						{sqltypes.NewInt64(1)},
						{sqltypes.NewVarBinary("aa")},
					},
				},
			},
			output: "select * from a where b = pk in (1, 'aa')",
		}, {
			desc:  "multi column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []ColIdent{NewColIdent("pk1"), NewColIdent("pk2")},
					Rows: [][]sqltypes.Value{
						{
							sqltypes.NewInt64(1),
							sqltypes.NewVarBinary("aa"),
						},
						{
							sqltypes.NewInt64(2),
							sqltypes.NewVarBinary("bb"),
						},
					},
				},
			},
			output: "select * from a where b = (pk1 = 1 and pk2 = 'aa') or (pk1 = 2 and pk2 = 'bb')",
		},
	}

	for _, tcase := range tcases {
		tree, err := Parse(tcase.query)
		if err != nil {
			t.Errorf("parse failed for %s: %v", tcase.desc, err)
			continue
		}
		buf := NewTrackedBuffer(nil)
		buf.Myprintf("%v", tree)
		pq := buf.ParsedQuery()
		bytes, err := pq.GenerateQuery(tcase.bindVars, tcase.extras)
		var got string
		if err != nil {
			got = err.Error()
		} else {
			got = string(bytes)
		}
		if got != tcase.output {
			t.Errorf("for test case: %s, got: '%s', want '%s'", tcase.desc, got, tcase.output)
		}
	}
}

func TestGenerateQueryForArgs(t *testing.T) {
	tcases := []struct {
		desc     string
		query    string
		bindVars []interface{}
		output   string
	}{
		{
			desc:  "args reuse",
			query: `INSERT INTO webcal_entry_recurrencerule(cal_id,cal_frequency,cal_interval,cal_byday,cal_bymonth,cal_bymonthday,cal_bysetpos,cal_count,cal_enddate) VALUES (?,?,?,?,?,?,?,?,?) on duplicate key update cal_frequency=?,cal_interval=?,cal_byday=?,cal_bymonth=?,cal_bymonthday=?,cal_bysetpos=?,cal_count=?,cal_enddate=?`,
			//query: `merge into "webcal_entry_recurrencerule" as "t" using "dual" on "t"."cal_id" = :v1 when matched then update set "t"."cal_frequency" = :v10, "t"."cal_interval" = :v11, "t"."cal_byday" = :v12, "t"."cal_bymonth" = :v13, "t"."cal_bymonthday" = :v14, "t"."cal_bysetpos" = :v15, "t"."cal_count" = :v16, "t"."cal_enddate" = :v17 when not matched then insert ("cal_id", "cal_frequency", "cal_interval", "cal_byday", "cal_bymonth", "cal_bymonthday", "cal_bysetpos", "cal_count", "cal_enddate") values (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9)`,
			bindVars: []interface{}{
				6666808, `daily`, 1, ``, ``, ``, ``, 0, 0, `daily`, 1, ``, ``, ``, ``, 0, 0,
			},
			output: `merge into "webcal_entry_recurrencerule" as "t" using "dual" on "t"."cal_id" = 6666808 when matched then update set "t"."cal_frequency" = 'daily', "t"."cal_interval" = 1, "t"."cal_byday" = '', "t"."cal_bymonth" = '', "t"."cal_bymonthday" = '', "t"."cal_bysetpos" = '', "t"."cal_count" = 0, "t"."cal_enddate" = 0 when not matched then insert ("cal_id", "cal_frequency", "cal_interval", "cal_byday", "cal_bymonth", "cal_bymonthday", "cal_bysetpos", "cal_count", "cal_enddate") values (6666808, 'daily', 1, '', '', '', '', 0, 0)`,
		},
	}

	for _, tcase := range tcases {
		tree, err := Parse(tcase.query)
		if err != nil {
			t.Errorf("parse failed for %s: %v", tcase.desc, err)
			continue
		}
		converter := NewOracleConverter(map[string]map[string][]string{
			"webcal_entry_recurrencerule": {
				"PRIMARY": {"cal_id"},
			},
		}, nil, nil)
		convertTree, _ := converter.convertStmt(tree)
		if convertTree == nil {
			t.Errorf("convert failed: %s", tcase.query)
			continue
		}

		pq := NewParsedQuery(convertTree)
		bytes, err := pq.GenerateQueryForArgs(tcase.bindVars)
		var got string
		if err != nil {
			got = err.Error()
		} else {
			got = string(bytes)
		}
		got = converter.replaceCommonIdents(got)
		if got != tcase.output {
			t.Errorf("for test case: %s, got: '%s', want '%s'", tcase.desc, got, tcase.output)
		} else {
			t.Logf("result sql: %s", got)
		}

	}
}
