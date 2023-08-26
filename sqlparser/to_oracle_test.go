package sqlparser

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestConvertToOracle(t *testing.T) {
	Init(map[string][]string{
		"conf_infos": {"eventId", "conferenceId"},
	})

	testCases := []struct {
		in, out string
	}{
		{
			in:  `INSERT INTO webcal_entry_recurrencerule(cal_id,cal_frequency,cal_interval,cal_byday,cal_bymonth,cal_bymonthday,cal_bysetpos,cal_count,cal_enddate) VALUES (?,?,?,?,?,?,?,?,?) on duplicate key update cal_frequency=?,cal_interval=?,cal_byday=?,cal_bymonth=?,cal_bymonthday=?,cal_bysetpos=?,cal_count=?,cal_enddate=?`,
			out: `merge into "webcal_entry_recurrencerule" as "t" using "dual" on "t"."cal_id" = :v1 when matched then update set "t"."cal_frequency" = :v10, "t"."cal_interval" = :v11, "t"."cal_byday" = :v12, "t"."cal_bymonth" = :v13, "t"."cal_bymonthday" = :v14, "t"."cal_bysetpos" = :v15, "t"."cal_count" = :v16, "t"."cal_enddate" = :v17 when not matched then insert ("cal_id", "cal_frequency", "cal_interval", "cal_byday", "cal_bymonth", "cal_bymonthday", "cal_bysetpos", "cal_count", "cal_enddate") values (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9)`,
		},
		//{
		//	in:  `insert into webcal_live_info(cal_id,channelId,pullurl,password,extraInfo) values(634311,131722,'https://rlive1uat.rmeet.com.cn/activity/geeZWo3','','') on duplicate key update pullurl='https://rlive1uat.rmeet.com.cn/activity/geeZWo3', password='', extraInfo=''`,
		//	out: `merge into webcal_live_info as t using dual on t.cal_id = 634311 and t.channelId = 131722 when matched then update set t.pullurl = 'https://rlive1uat.rmeet.com.cn/activity/geeZWo3', t.password = '', t.extraInfo = '' when not matched then insert (cal_id, channelId, pullurl, password, extraInfo) values (634311, 131722, 'https://rlive1uat.rmeet.com.cn/activity/geeZWo3', '', '')`,
		//},
		//{
		//	in:  `REPLACE INTO exchange_bindinfo (userId,resId,bindingData) values (1,"abcd","101003")`,
		//	out: `merge into exchange_bindinfo as t using dual on t.userId = 1 when matched then update set t.resId = 'abcd', t.bindingData = '101003' when not matched then insert (userId, resId, bindingData) values (1, 'abcd', '101003')`,
		//},
		//{
		//	in:  "SELECT cal_id, creator, url, startTime, endTime, createTime, updateTime, title, type FROM conf_summary WHERE `type`=4 AND cal_id=635427;",
		//	out: "SELECT cal_id, creator, url, startTime, endTime, createTime, updateTime, title, type FROM conf_summary WHERE \"type\"=4 AND cal_id=635427;",
		//},
		//{
		//	in:  `insert into conf_infos (conferenceId,billingCode,pcode1,pcode2,hostJoinUrl,attendeeJoinUrl,joinHostUrl,guestJoinUrl,audienceJoinUrl,audienceUnionUrl,wcallMonitorUrl,eventId,confType,accessNumbers,btplRole,thirdConfId) VALUES ('239816817','95503974','201501000037705193','201501000037705194','https://stest.qsh1.cn/a/HV2GkXD29153','https://ntest.qsh1.cn/k/mLeyHKKu9re','https://stest.qsh1.cn/a/GV2GkX3C4163','https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgODeUYP4adFq0lkE2_OnlVb5d25p3aU1DP263VGa6Og0','https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgFEoJJ-bWCLvHngnwjHAgJUkY-Eoic6Htt7g-7VbPvq0','https://rlive1uat.rmeet.com.cn/activity/gefjbGl','',635497,4,'null','','')  on duplicate key update confType=4`,
		//	out: `merge into conf_infos as t using dual on t.conferenceId = '239816817' and t.eventId = 635497 when matched then update set t.confType = 4 when not matched then insert (conferenceId, billingCode, pcode1, pcode2, hostJoinUrl, attendeeJoinUrl, joinHostUrl, guestJoinUrl, audienceJoinUrl, audienceUnionUrl, wcallMonitorUrl, eventId, confType, accessNumbers, btplRole, thirdConfId) values ('239816817', '95503974', '201501000037705193', '201501000037705194', 'https://stest.qsh1.cn/a/HV2GkXD29153', 'https://ntest.qsh1.cn/k/mLeyHKKu9re', 'https://stest.qsh1.cn/a/GV2GkX3C4163', 'https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgODeUYP4adFq0lkE2_OnlVb5d25p3aU1DP263VGa6Og0', 'https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgFEoJJ-bWCLvHngnwjHAgJUkY-Eoic6Htt7g-7VbPvq0', 'https://rlive1uat.rmeet.com.cn/activity/gefjbGl', '', 635497, 4, 'null', '', '')`,
		//},
		//{
		//	in:  "update meet_stop_job  set `mark` = '172.10.157.179',`updateTime`='2023-07-13 00:00:00' where stopTime <= 1689177600 and serverUrl='http://uniform.quanshi.com'",
		//	out: `update meet_stop_job  set "mark" = '172.10.157.179',"updateTime"='2023-07-13 00:00:00' where stopTime <= 1689177600 and serverUrl='http://uniform.quanshi.com'`,
		//},
		//{
		//	in:  "delete from meet_conference_extrainfo where `conferenceId` = '239816811'",
		//	out: `delete from meet_conference_extrainfo where "conferenceId" = '239816811'`,
		//},
	}

	converter := NewOracleConverter(map[string]map[string][]string{
		"webcal_live_info": {
			"cal_id":        {"cal_id"},
			"CONS134222551": {"channelId"},
		},
		"webcal_entry_recurrencerule": {
			"PRIMARY": {"cal_id"},
		},
		"exchange_bindinfo": {
			"user_id": {"userId"},
			"resId":   {"resId"},
		},
	}, nil, nil)
	for i, tcase := range testCases {
		t.Run(fmt.Sprintf("testcase-%d", i+1), func(t *testing.T) {
			oSql, _, err := converter.Convert(tcase.in)
			assert.Nil(t, err)
			assert.Equal(t, tcase.out, oSql)
			log.Println(oSql)
		})
	}

}
