package backend

import (
	"sqlproxy/config"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

var testdb *BackendProxy

func TestMain(m *testing.M) {
	cfg := config.NodeConfig{
		Name:         "uc_uniform",
		DriverName:   "mysql", // "godror"
		Datasource:   "proxy_dml:quanshitestcmnew@tcp(192.168.39.119:3306)/uc_uniform?charset=utf8mb4&readTimeout=10s&writeTimeout=10s",
		MaxOpenConns: 1,
		TestSQL:      "select * from webcal_entry limit 1;",
	}
	testdb = NewBackendProxy(cfg)
	err := testdb.InitConnectionPool()
	if err != nil {
		panic(err)
	}
	m.Run()
}

func TestExec(t *testing.T) {
	db := testdb
	res, err := db.Exec(`insert into webcal_live_info(cal_id,channelId,pullurl,password,extraInfo) values(3,131722,'https://rlive1uat.rmeet.com.cn/activity/geeZWo3','','{"liveViewFlag":0,"livePlaybackFlag":0,"livePlaybackTime":0,"jointHostUrl":"https://stest.qsh1.cn/a/GVaZkX26ACE2"}') on duplicate key update password='', extraInfo='{"liveViewFlag":0,"livePlaybackFlag":0,"livePlaybackTime":0,"jointHostUrl":"https://stest.qsh1.cn/a/GVaZkX26ACE2"}'`)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	t.Logf("affectedRows: %d, insertId: %d", res.AffectedRows, res.InsertId)
}

func TestTransaction(t *testing.T) {
	db := testdb
	tx, err := db.Begin()
	assert.Nil(t, err)
	assert.NotNil(t, tx)

	rs, err := tx.Exec("update webcal_id set id=id+1 WHERE type=?;", 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, int(rs.AffectedRows))
	assert.True(t, rs.InsertId == 0)

	rs, err = tx.Query("SELECT id FROM webcal_id WHERE type=?", 1)
	assert.Nil(t, err)
	assert.NotNil(t, rs)
	assert.NotNil(t, rs.Resultset)
	assert.Equal(t, 1, rs.Resultset.ColumnNumber())
	assert.Equal(t, 1, rs.Resultset.RowNumber())

	val, err := rs.GetUint(0, 0)
	assert.Nil(t, err)
	assert.True(t, val > 0)
	err = tx.Commit()
	assert.Nil(t, err)
	t.Logf("webcal_id: %v", val)
}

func TestPrepareStatement(t *testing.T) {
	db := testdb
	rs, err := db.Exec("INSERT INTO webcal_entry_recurrencerule(cal_id,cal_frequency,cal_interval,cal_byday,cal_bymonth,cal_bymonthday,cal_bysetpos,cal_count,cal_enddate) VALUES (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9) on duplicate key update cal_frequency= :v10,cal_interval= :v11,cal_byday= :v12,cal_bymonth= :v13,cal_bymonthday= :v14,cal_bysetpos= :v15,cal_count= :v16,cal_enddate= :v17",
		6666808, `daily`, 1, ``, ``, ``, ``, 0, 0, `daily`, 1, ``, ``, ``, ``, 0, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, int(rs.AffectedRows))
	assert.True(t, rs.InsertId == 0)
}
