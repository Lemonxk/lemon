# coding:utf-8
'''
@author: admin
'''

import hconfig
from utils import conn2mysql
from impala.dbapi import connect
from hiveExe2 import HiveQuery
import datetime, time, sys, traceback
from dateFormat import customTime

class reportUserDayAppPlatform(object):
    def __init__(self, dttime, appid=0, platform=0):
        self.dttime = dttime
        self.appid = appid
        self.platform = platform
        self.hive_conn = HiveQuery()
        self.roomonline_hql_arr = []
        self.conn, self.cursor = conn2mysql(hconfig.mysqlhost, hconfig.mysqlport, hconfig.mysqluser, hconfig.mysqlpwd,
                                            hconfig.mysqldatabase)
        self.impalaconn = connect(host=hconfig.impalahost)
        self.impalacur = self.impalaconn.cursor()
        self.impalacur.execute("refresh api_db_user_log")
        self.impalacur.execute("refresh api_db_user_info")
        self.impalacur.execute("refresh room_inout")
        self.current_date = datetime.datetime.strptime(self.dttime, "%Y%m%d").date()
        self.now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        self.dttime_begin = ((self.dttime)[0:6] + '01')
        self.activeuser_hql = ''
        self.un_activenewuser_hql = ''
        self.newuser_hql = ''
        self.whole_user_hql = ''
        self.visitnum_hql = ''
        self.platformSql = ''
        self.vistnum_web_hql = ''
        self.watch_time_90_hql = ""
        self.watch_time_300_hql = ""
        self.hql_generater()

    def hql_generater(self):
        if (self.platform > 0):
            # iphone和ipad合并在一起
            if (self.platform == 3):
                self.platformSql = 'in (3,4) '
            else:
                self.platformSql = '= %s' % (self.platform)
        else:
            self.platformSql = '>= 0'

        # 累计非新活跃用户
        self.un_activenewuser_hql = '''  select count( distinct a.userid) from (
             select userid from api_db_user_log ul where ul.dt>='%s' and ul.dt<='%s'
            ) a join (
            select userid from api_db_user_info ui  where ui.openplatform != 0
            AND ui.openplatform != - 5 and ui.dt <'%s' and ui.userid is not null
            and ui.terminal %s
            ) b on a.userid = b.userid
            ''' % (self.dttime_begin, self.dttime,self.dttime_begin, self.platformSql)

        # newuser新增用户
        self.newuser_hql = '''
            select count(DISTINCT userid) from api_db_user_info
            where openplatform != 0 AND openplatform != - 5  and dt= '%s' and terminal %s
            ''' % (self.dttime,self.platformSql)

        #totaluser累积用户
        self.whole_user_hql = '''
            select count(distinct userid) from api_db_user_info
            where dt >='20120901' and dt <= '%s' and
            openplatform != 0 and openplatform != - 5 and terminal %s
            ''' % (self.dttime, self.platformSql)

        self.visitnum_hql = '''
            select count(distinct userid) from api_db_guest_log where dt = '%s' and platform %s
            ''' % (self.dttime, self.platformSql)

        self.vistnum_web_hql = '''
            select count(distinct guestuid) from room_inout  where dt = '%s' and msgtag = 2
            ''' % (self.dttime)

        # activeuser活跃用户
        self.activeuser_hql = '''
            select  count(DISTINCT al.userid) from  api_db_user_log al, api_db_user_info ai
            where al.userid=ai.userid and  al.dt='%s'  and ai.terminal %s
            and ai.openplatform != 0 AND ai.openplatform != - 5
            ''' % (self.dttime, self.platformSql)

        # 分钟级别最高在线人数
        self.pcu_sql = '''
            select max(num) from(
                select sum(max_num) as num, hour, minute from (
                 select roomid, hour, minute, max_num from report_room_online_minute
                 where dt='%s' and appid=1 and platform %s
                ) a group by hour, minute
                ) b
            ''' % (self.dttime, self.platformSql)

        self.roomonline_hql_arr.append('add jar hdfs:///user/function/RoomOnlineMin.jar')
        self.roomonline_hql_arr.append(
            "create temporary function room_online as 'com.melot.function.room.RoomOnlineMin'")
        hql = '''select room_online(a.stime, a.etime, a.userid) from
                    (
                    SELECT it.userid, it.roomid, it.rownum as etime, it.recordtime as stime
                    FROM
                    (SELECT io.userid, io.roomid, io.recordtime, io.msgtag,  io.dt,
                      LEAD (io.recordtime, 1) OVER (PARTITION BY io.userid, io.roomid,io.platform
                    ORDER BY io.userid, io.recordtime ) rownum,
                    LEAD (io.msgtag, 1) OVER ( PARTITION BY io.userid, io.roomid,io.platform
                    ORDER BY io.userid, io.recordtime ) nexttag
                    FROM  room_inout io WHERE dt = '%s' and platform %s  and userid!=0 and room_type=1 ) it
                    WHERE it.msgtag IN (3) AND it.nexttag IN (4)
                    )  a ''' % (self.dttime, self.platformSql)
        self.roomonline_hql_arr.append(hql)

        self.pcu_sql_all = '''
            select max(num) from(
                select sum(max_num) as num, hour, minute from (
                 select roomid, hour, minute, max_num from report_room_online_minute
                 where dt='%s' and appid=1 and platform =0
                ) a group by hour, minute
                ) b
            ''' % (self.dttime)

        self.watch_time_90_hql = '''
            SELECT
              pv.platform, count(distinct pv.userid)
            FROM
              (SELECT it.userid, it.roomid, it.rownum, it.recordtime, it.appid, it.platform,it.openplatform
              FROM
                (SELECT io.userid, io.roomid, io.recordtime, io.msgtag,  io.dt, io.appid, io.platform,io.openplatform,
                  LEAD (io.recordtime, 1) OVER ( PARTITION BY io.userid, io.roomid,io.platform
                  ORDER BY io.userid, io.recordtime ) rownum,
                  LEAD (io.msgtag, 1) OVER ( PARTITION BY io.userid, io.roomid,io.platform
                  ORDER BY io.userid, io.recordtime ) nexttag
                FROM  room_inout io WHERE dt = '%s') it
                WHERE it.msgtag IN (3) AND it.nexttag IN (4)) pv
            WHERE pv.openplatform!=0 AND ( UNIX_TIMESTAMP(pv.rownum) - UNIX_TIMESTAMP(pv.recordtime) ) >= 90
            GROUP BY pv.platform ''' % (self.dttime)

        self.watch_time_300_hql = '''
            SELECT
              pv.platform, count(distinct pv.userid)
            FROM
              (SELECT it.userid, it.roomid, it.rownum, it.recordtime, it.appid, it.platform,it.openplatform
              FROM
                (SELECT io.userid, io.roomid, io.recordtime, io.msgtag,  io.dt, io.appid, io.platform,io.openplatform,
                  LEAD (io.recordtime, 1) OVER ( PARTITION BY io.userid, io.roomid,io.platform
                  ORDER BY io.userid, io.recordtime ) rownum,
                  LEAD (io.msgtag, 1) OVER ( PARTITION BY io.userid, io.roomid,io.platform
                  ORDER BY io.userid, io.recordtime ) nexttag
                FROM room_inout io WHERE dt = '%s') it
                WHERE it.msgtag IN (3) AND it.nexttag IN (4)) pv
            WHERE pv.openplatform!=0 AND ( UNIX_TIMESTAMP(pv.rownum) - UNIX_TIMESTAMP(pv.recordtime) ) >= 300
            GROUP BY pv.platform ''' % (self.dttime)

    def newuser(self):
        try:
            self.impalacur.execute(self.newuser_hql)
            result = self.impalacur.fetchall()
            for value in result:
                newuser = value[0]
                self.insert()
                self.cursor.execute(
                    "UPDATE  report_day SET user_newuser =%s WHERE dt=%s and appid=%s and platform=%s" % (
                        str(newuser), self.dttime, self.appid, self.platform))
                self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)

    def whole_user(self):
        try:
            self.impalacur.execute(self.whole_user_hql)
            result = self.impalacur.fetchall()
            for line in result:
                totaluser = line[0]
                self.insert()
                self.cursor.execute("UPDATE  report_day SET user_totaluser=%s WHERE dt=%s and appid=%s and platform=%s" % (
                    str(totaluser), self.dttime, self.appid, self.platform))
                self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)

    def visit_user(self):
        try:
            self.impalacur.execute(self.visitnum_hql)
            result = self.impalacur.fetchall()
            for value in result:
                visitnum = value[0]
                self.insert()
                self.cursor.execute(
                    "UPDATE  report_day SET visitornum =%s WHERE dt=%s and appid=%s and platform=%s" % (
                    visitnum, self.dttime, self.appid, self.platform))
            self.impalacur.execute(self.vistnum_web_hql)
            results = self.impalacur.fetchall()
            for line in results:
                visitnum_web = line[0]
                self.cursor.execute(
                    "UPDATE  report_day SET visitornum =%s WHERE dt=%s and appid=%s and platform=%s" % (
                    visitnum_web, self.dttime, 1, 1))
            self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)

    def activeuser(self):
        try:
            self.impalacur.execute(self.activeuser_hql)
            result = self.impalacur.fetchall()
            for value in result:
                activeuser = value[0]
                self.insert()
                update_sql = '''UPDATE  report_day SET user_activeuser=%s WHERE dt=%s and appid=%s and platform=%s ''' % (
                str(activeuser), self.dttime, self.appid, self.platform)
                self.cursor.execute(update_sql)
            self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)

    def pcu(self):
        try:
            arrs = []
            self.hive_conn.execute_cmd(self.roomonline_hql_arr)
            print  self.roomonline_hql_arr
            client, transport = self.hive_conn.get_connandtransport()
            sqlResult = client.fetch()
            if sqlResult:
                for value in sqlResult:
                    online_data = value[0].split('\t')
                    maxArr = online_data[1:1441]
                    for i in range(1440):
                        arrs.append(int(maxArr[i]))
                    self.insert()
                    self.cursor.execute(
                        "UPDATE report_day SET pcu=%s WHERE dt=%s and appid=%s and platform=%s"
                        % (max(arrs), self.dttime, self.appid, self.platform))
            self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)

    def insert(self):
        try:
            self.cursor.execute("SELECT COUNT(*) FROM report_day WHERE dt='%s' AND appid =%s and platform=%s" % (
                self.dttime, self.appid, self.platform))
            result = self.cursor.fetchone()
            dataNum = result[0]
            if dataNum < 1L:
                self.cursor.execute(
                    "INSERT INTO report_day(id,dt,appid,recordtime,platform) VALUES(NULL,%s,%s,'%s',%s)" % (
                        self.dttime, self.appid, self.now, self.platform))
                self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)

    def un_activenewuser(self):
        try:
            self.impalacur.execute(self.un_activenewuser_hql)
            result = self.impalacur.fetchall()
            for value in result:
                cumulative_un_newuser_num = value[0]
                self.insert()
                self.cursor.execute(
                    "UPDATE  report_day SET cumulative_un_newuser_num =%s WHERE dt=%s and appid=%s and platform=%s" % (
                        str(cumulative_un_newuser_num), self.dttime, self.appid, self.platform))
                self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)

    def watch_time_90(self):
        try:
            self.impalacur.execute(self.watch_time_90_hql)
            result = self.impalacur.fetchall()
            watch_time_90_all = 0
            watch_time_90_3 = 0
            watch_time_90_4 = 0
            for value in result:
                platform = value[0]
                watch_time_90 = value[1]
                watch_time_90_all = watch_time_90_all + watch_time_90
                if platform == 1:
                    self.cursor.execute(
                    "UPDATE  report_day SET watch_time_90 =%s  WHERE dt='%s' and appid=1 and platform=%s" % (
                        watch_time_90, self.dttime, platform))
                if platform == 2:
                    self.cursor.execute(
                    "UPDATE  report_day SET watch_time_90 =%s  WHERE dt='%s' and appid=1 and platform=%s" % (
                        watch_time_90, self.dttime, platform))
                if platform == 3:
                    watch_time_90_3 = watch_time_90
                if platform == 4:
                    watch_time_90_4 = watch_time_90
            watch_time_90_l =  watch_time_90_3 + watch_time_90_4
            self.cursor.execute(
                    "UPDATE  report_day SET watch_time_90 =%s  WHERE dt='%s' and appid=1 and platform=3" % (
                        watch_time_90_l, self.dttime))
            self.cursor.execute(
                    "UPDATE  report_day SET watch_time_90 =%s  WHERE dt='%s' and appid=1 and platform=0" % (
                        watch_time_90_all, self.dttime))
            self.conn.commit()
            time.sleep(2)
        except Exception, e:
            print e
            print 1 + str(e)

    def watch_time_300(self):
        try:
            self.impalacur.execute(self.watch_time_300_hql)
            result = self.impalacur.fetchall()
            watch_time_300_all = 0
            watch_time_300_3 = 0
            watch_time_300_4 = 0
            for value in result:
                platform = value[0]
                watch_time_300 = value[1]
                watch_time_300_all = watch_time_300_all + watch_time_300
                if platform == 1:
                    self.cursor.execute(
                    "UPDATE  report_day SET watch_time_300 =%s  WHERE dt='%s' and appid=1 and platform=%s" % (
                        watch_time_300, self.dttime, platform))
                if platform == 2:
                    self.cursor.execute(
                    "UPDATE  report_day SET watch_time_300 =%s  WHERE dt='%s' and appid=1 and platform=%s" % (
                        watch_time_300, self.dttime, platform))
                if platform == 3:
                    watch_time_300_3 = watch_time_300
                if platform == 4:
                    watch_time_300_4 = watch_time_300
            watch_time_300_l =  watch_time_300_3 + watch_time_300_4
            self.cursor.execute(
                    "UPDATE  report_day SET watch_time_300 =%s  WHERE dt='%s' and appid=1 and platform=3" % (
                        watch_time_300_l, self.dttime))
            self.cursor.execute(
                    "UPDATE  report_day SET watch_time_300 =%s  WHERE dt='%s' and appid=1 and platform=0" % (
                        watch_time_300_all, self.dttime))
            self.conn.commit()
            time.sleep(2)
        except Exception, e:
            print e
            print 1 + str(e)

    def report(self):
        self.newuser()
        self.activeuser()
        self.pcu()
        self.whole_user()
        self.visit_user()
        self.un_activenewuser()
        self.watch_time_90()
        self.watch_time_300()

    def call_report(self):
        reportUserDayAppPlatform(self.dttime, 1, 0).report()
        reportUserDayAppPlatform(self.dttime, 1, 1).report()
        reportUserDayAppPlatform(self.dttime, 1, 2).report()
        reportUserDayAppPlatform(self.dttime, 1, 3).report()

def main():
    dt = customTime(1)
    for dttime in dt:
        print dttime
        reportUserDayAppPlatform(dttime).call_report()

if __name__ == '__main__':
    main()
