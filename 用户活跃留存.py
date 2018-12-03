# coding:utf-8
'''
@author: admin
'''
import hconfig
from utils import conn2mysql, conn2Impala, kkOption
import datetime
import sys
import traceback
from dateFormat import customTime
class ReportUserWeek(object):
    def __init__(self, dttime, appid, platform):
        self.conn, self.cursor = conn2mysql(hconfig.mysqlhost, hconfig.mysqlport, hconfig.mysqluser, hconfig.mysqlpwd,
                                            hconfig.mysqldatabase)
        self.impalaconn, self.impalacur = conn2Impala()
        self.dttime = dttime
        self.appid = appid
        self.platform = platform
        self.current_date = datetime.datetime.strptime(self.dttime, "%Y%m%d").date()
        self.last_week_end_dttime = self.dttime
        self.last_week_start_dttime = (self.current_date - datetime.timedelta(days=6)).strftime("%Y%m%d")
        self.last_two_week_end_dttime = (self.current_date - datetime.timedelta(days=7)).strftime("%Y%m%d")
        self.last_two_week_start_dttime = (self.current_date - datetime.timedelta(days=13)).strftime("%Y%m%d")
        self.last_three_week_end_dttime = (self.current_date - datetime.timedelta(days=14)).strftime("%Y%m%d")
        self.last_three_week_start_dttime = (self.current_date - datetime.timedelta(days=20)).strftime("%Y%m%d")
        self.active_user_hql = ''
        self.newuser_hql = ''
        self.retention_hql = ''
        self.loss_hql = ''
        self.reflux_hql = ''
        self.silent_reflux_hql = ''
        self.new_user_hql = ''
        self.appSql = ''
        self.platformSql = ''
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
        # 活跃用户
        self.active_user_hql = '''
        SELECT count(DISTINCT userid) FROM  api_db_user_log WHERE dt>='%s' AND dt<='%s'
        AND userid IN(SELECT DISTINCT userid FROM api_db_user_info
            WHERE openplatform != 0 AND openplatform != - 5
            AND terminal %s and dt<='%s' and userid is not null)
        ''' % (self.last_week_start_dttime, self.last_week_end_dttime,
               self.platformSql, self.last_week_end_dttime)
        #留存用户
        self.retention_hql = '''
        select count(distinct lo.userid) as ct from api_db_user_log as lo
        left join (select distinct userid from api_db_user_log b where b.dt >= '%s' and b.dt <= '%s'
        and b.userid in (SELECT DISTINCT userid FROM api_db_user_info
            WHERE openplatform != 0 AND openplatform != - 5
            AND terminal %s and dt <='%s' and userid is not null))
        as a on lo.userid = a.userid
        where a.userid is not null
        and lo.dt >= '%s'
        and lo.dt <= '%s'
        ''' % (self.last_two_week_start_dttime, self.last_two_week_end_dttime, self.platformSql,
               self.last_two_week_end_dttime, self.last_week_start_dttime, self.last_week_end_dttime)
        #流失用户
        self.loss_hql = '''
        SELECT count(DISTINCT t1.userid) FROM api_db_user_log t1 WHERE t1.dt >= '%s' AND t1.dt <= '%s'
        AND t1.userid in (SELECT DISTINCT userid FROM api_db_user_info
            WHERE openplatform != 0 AND openplatform != - 5
            AND terminal %s and dt <='%s' and userid is not null)
        and t1.userid not in(
            SELECT DISTINCT userid FROM api_db_user_log t2 WHERE dt >= '%s' and dt <= '%s'
            and userid in (SELECT DISTINCT userid FROM api_db_user_info
            WHERE openplatform != 0 AND openplatform != - 5
            AND terminal %s and dt <='%s' and userid is not null)
        )
        ''' % (self.last_two_week_start_dttime, self.last_two_week_end_dttime, self.platformSql, self.last_two_week_end_dttime,
               self.last_week_start_dttime, self.last_week_end_dttime, self.platformSql, self.last_two_week_end_dttime)
        #回流用户
        self.reflux_hql = '''
        select count(distinct lo.userid) as ct from api_db_user_log as lo
        left join (select distinct userid from api_db_user_log b where b.dt >= '%s' and b.dt <= '%s')as a on lo.userid = a.userid  where lo.userid in
        (select distinct userid from api_db_user_log c where c.dt >= '%s' and c.dt <= '%s'
        and c.userid in (SELECT DISTINCT userid FROM api_db_user_info
        WHERE openplatform != 0 AND openplatform != - 5
        AND terminal %s and dt <='%s' and userid is not null))
        and a.userid is null
        and lo.dt >= '%s'
        and lo.dt <= '%s'
        ''' % (self.last_two_week_start_dttime, self.last_two_week_end_dttime,
               self.last_three_week_start_dttime, self.last_three_week_end_dttime,
               self.platformSql, self.last_three_week_end_dttime, self.last_week_start_dttime, self.last_week_end_dttime)
        #沉默回流用户
        self.silent_reflux_hql = '''
        select count(distinct lo.userid) as ct from api_db_user_log as lo
        left join (select distinct userid from api_db_user_log b where b.dt >= '%s' and b.dt <= '%s'
        and userid in(select DISTINCT userid
        FROM api_db_user_info
        where openplatform != 0
        AND openplatform != - 5
        AND terminal %s and dt <='%s' and userid is not null))
        as a on lo.userid = a.userid
        left join
        (select distinct userid
        from api_db_user_info
        where dt>='%s'and dt<='%s'
        and  openplatform != 0
        AND openplatform != - 5
        AND terminal %s  and userid is not null)c
        on lo.userid=c.userid
        where a.userid is null
        and c.userid is null
        and lo.userid in (select DISTINCT userid
        FROM api_db_user_info
        where openplatform != 0
        AND openplatform != - 5
        AND terminal %s and dt <='%s' and userid is not null)
        and lo.dt >= '%s'
        and lo.dt <= '%s'
        ''' % (self.last_three_week_start_dttime, self.last_two_week_end_dttime, self.platformSql, self.last_three_week_end_dttime,
               self.last_week_start_dttime, self.last_week_end_dttime, self.platformSql,
               self.platformSql, self.last_three_week_end_dttime, self.last_week_start_dttime, self.last_week_end_dttime)
        #新增用户
        self.new_user_hql = '''
        SELECT count(DISTINCT userid)
        FROM api_db_user_info WHERE openplatform != 0 AND openplatform != - 5
        and dt>='%s' and dt<='%s'
        AND terminal %s  and userid is not null
        ''' % (self.last_week_start_dttime, self.last_week_end_dttime, self.platformSql)
        # APP游客
        self.visitor_user_hql_app = '''
        select count(distinct a.userid) from (
        select distinct userid from api_db_guest_log where dt>='%s' and dt<='%s' and platform %s
        )a
        left join (
        select userid from api_db_user_info where dt >= '%s' and dt <= '%s' and openplatform not in (0,-5)
        and userid is not null
        )b on a.userid = b.userid where b.userid is null
        ''' % (self.last_week_start_dttime, self.last_week_end_dttime, self.platformSql,
               self.last_week_start_dttime, self.last_week_end_dttime,)
        # WEB游客
        self.visitor_user_hql_web = '''
        select count(distinct guestuid) from room_inout where dt>='%s' and dt<='%s' and msgtag = 2
        ''' % (self.last_week_start_dttime, self.last_week_end_dttime)
    # 活跃
    def active_user(self):
        try:
            print self.active_user_hql
            self.impalacur.execute(self.active_user_hql)
            result = self.impalacur.fetchall()
            for line in result:
                activeuser = line[0]
                self.insert("user_active_user", activeuser, self.last_week_start_dttime)
        except Exception, e:
            print e
            print 1 + str(e)
    # 留存
    def retention(self):
        try:
            print self.retention_hql
            self.impalacur.execute(self.retention_hql)
            result = self.impalacur.fetchall()
            for line in result:
                num = line[0]
                self.insert("user_retention", num, self.last_week_start_dttime)
                self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)
    # 流失
    def loss(self):
        try:
            print self.loss_hql
            self.impalacur.execute(self.loss_hql)
            result = self.impalacur.fetchall()
            for line in result:
                num = line[0]
                self.insert("user_loss", num, self.last_week_start_dttime)
        except Exception, e:
            print e
            print 1 + str(e)
    # 回流
    def reflux(self):
        try:
            print self.reflux_hql
            self.impalacur.execute(self.reflux_hql)
            result = self.impalacur.fetchall()
            for line in result:
                num = line[0]
                self.insert("user_reflux", num, self.last_week_start_dttime)
        except Exception, e:
            print e
            print 1 + str(e)
    # 沉默回流
    def silent_reflux(self):
        try:
            print self.silent_reflux_hql
            self.impalacur.execute(self.silent_reflux_hql)
            result = self.impalacur.fetchall()
            for line in result:
                num = line[0]
                self.insert("user_silent_reflux", num, self.last_week_start_dttime)
                self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)
    # 新增
    def new_user(self):
        try:
            print self.new_user_hql
            self.impalacur.execute(self.new_user_hql)
            result = self.impalacur.fetchall()
            for line in result:
                num = line[0]
                self.insert("user_new_user", num, self.last_week_start_dttime)
        except Exception, e:
            print e
            print 1 + str(e)
    # 游客
    def visitor_user(self):
        try:
            visitor = 0
            #只有唱响有游客数
            if (self.appid == 1):
                #全平台
                if (self.platform == 0):
                    print self.visitor_user_hql_app
                    print self.visitor_user_hql_web
                    self.impalacur.execute(self.visitor_user_hql_app)
                    result1 = self.impalacur.fetchone()
                    self.impalacur.execute(self.visitor_user_hql_web)
                    result2 = self.impalacur.fetchone()
                    visitor = result1[0] + result2[0]
                #WEB
                elif (self.platform == 1):
                    print self.visitor_user_hql_web
                    self.impalacur.execute(self.visitor_user_hql_web)
                    visitor = self.impalacur.fetchone()[0]
                #APP
                else :
                    print self.visitor_user_hql_app
                    self.impalacur.execute(self.visitor_user_hql_app)
                    visitor = self.impalacur.fetchone()[0]
            self.insert("user_visitor", visitor, self.last_week_start_dttime)
        except Exception, e:
            print e
            print 1 + str(e)
    def insert(self, key, value, time):
        try:
            self.cursor.execute("SELECT COUNT(*) FROM report_week WHERE dt=%s and appid = %s and platform = %s" % (time, self.appid, self.platform))
            result = self.cursor.fetchone()
            dataNum = result[0]
            if dataNum < 1L:
                insert_sql = "INSERT INTO report_week(id,dt,appid,platform,%s) VALUES(NULL,%s,%s,%s,%s)" % (key, time, self.appid, self.platform, value)
                print insert_sql
                self.cursor.execute(insert_sql)
                self.conn.commit()
            else:
                update_sql = "UPDATE report_week SET %s = %s WHERE dt=%s and appid = %s and platform = %s" % (key, value, time, self.appid, self.platform)
                print update_sql
                self.cursor.execute(update_sql)
                self.conn.commit()
        except Exception, e:
            print e
            print 1 + str(e)
    def report(self):
        self.active_user()
        self.new_user()
        self.loss()
        self.retention()
        self.silent_reflux()
        self.reflux()
        self.visitor_user()
def main():
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #date_list = [ '20160110', '20160117', '20160124', '20160131', '20160207', '20160214', '20160221', '20160228','20160306']
    dt = customTime(1)
    for sunday in dt:
        ReportUserWeek(sunday, 1, 0).report()
        ReportUserWeek(sunday, 1, 1).report()
        ReportUserWeek(sunday, 1, 2).report()
        ReportUserWeek(sunday, 1, 3).report()
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
if __name__ == '__main__':
    main()
