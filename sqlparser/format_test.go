package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormat(t *testing.T) {
	tcases := []struct {
		desc     string
		query    string
		bindVars []interface{}
		output   string
	}{
		{
			desc:  "index args",
			query: `INSERT INTO webcal_entry_recurrencerule(cal_id,cal_frequency,cal_interval,cal_byday,cal_bymonth,cal_bymonthday,cal_bysetpos,cal_count,cal_enddate) VALUES (?,?,?,?,?,?,?,?,?) on duplicate key update cal_frequency=?,cal_interval=?,cal_byday=?,cal_bymonth=?,cal_bymonthday=?,cal_bysetpos=?,cal_count=?,cal_enddate=?`,
			bindVars: []interface{}{
				6666808, `daily`, 1, ``, ``, ``, ``, 0, 0, `daily`, 1, ``, ``, ``, ``, 0, 0,
			},
			output: `INSERT INTO webcal_entry_recurrencerule(cal_id,cal_frequency,cal_interval,cal_byday,cal_bymonth,cal_bymonthday,cal_bysetpos,cal_count,cal_enddate) VALUES (6666808,'daily',1,'','','','',0,0) on duplicate key update cal_frequency='daily',cal_interval=1,cal_byday='',cal_bymonth='',cal_bymonthday='',cal_bysetpos='',cal_count=0,cal_enddate=0`,
		},
		{
			desc:  "name args",
			query: `INSERT INTO webcal_entry_recurrencerule(cal_id,cal_frequency,cal_interval,cal_byday,cal_bymonth,cal_bymonthday,cal_bysetpos,cal_count,cal_enddate) VALUES (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9) on duplicate key update cal_frequency= :v10,cal_interval= :v11,cal_byday= :v12,cal_bymonth= :v13,cal_bymonthday= :v14,cal_bysetpos= :v15,cal_count= :v16,cal_enddate= :v17`,
			bindVars: []interface{}{
				6666808, `daily`, 1, ``, ``, ``, ``, 0, 0, `daily`, 1, ``, ``, ``, ``, 0, 0,
			},
			output: "insert into `webcal_entry_recurrencerule`(`cal_id`, `cal_frequency`, `cal_interval`, `cal_byday`, `cal_bymonth`, `cal_bymonthday`, `cal_bysetpos`, `cal_count`, `cal_enddate`) values (6666808, 'daily', 1, '', '', '', '', 0, 0) on duplicate key update `cal_frequency` = 'daily', `cal_interval` = 1, `cal_byday` = '', `cal_bymonth` = '', `cal_bymonthday` = '', `cal_bysetpos` = '', `cal_count` = 0, `cal_enddate` = 0",
		},
	}

	for _, tcase := range tcases {
		got, err := Format(tcase.query, tcase.bindVars)
		assert.Nil(t, err)
		assert.Equal(t, tcase.output, got)
	}
}
