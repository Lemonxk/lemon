# coding:utf-8

import os, datetime, calendar, time
import sys,traceback
from common import logger,hconfig
from common.utils import conn2PG, conn2mysql, conn2PG_melotlog, conn2PG_melotpay
from common.mysql_dao_new import MysqlDao
import MySQLdb

class FinanceMonthCategoryStats(object):
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

    def __init__(self, dttime):
        self.dttime = dttime
        self.first,self.last=self.get_first_lastday(dttime)
        self.dao = MysqlDao()
        self.pg_conn, self.pg_cursor = conn2PG()
        self.melotlog_conn, self.melotlog_cursor = conn2PG_melotlog()
        self.melotpay_conn, self.melotpay_cursor = conn2PG_melotpay()
        self.game_conn = MySQLdb.connect(host='192.168.22.10',port=3306, user='kkgame', passwd='kkgame', db='kkgame', charset='utf8')
        self.game_cursor = self.game_conn.cursor()

    def get_first_lastday(self, time):
        date = datetime.datetime.strptime(time, "%Y%m%d").date()
        year = date.year
        month = date.month
        if month == 1:
            month = 12
            year -= 1
        else:
            month -= 1
        d = calendar.monthrange(year, month)
        return datetime.date(year, month, 1).strftime("%Y%m%d"), datetime.date(year, month+1, 1).strftime("%Y%m%d")

    #消耗支出统计
    def consume_amounts(self):
        infraction_map={0:"规处理",1:"家族等级",2:"家族保证金",3:"k豆转秀币",4:"清空官方主播K豆",5:"解散家族保证金发放",6:"家族(拖欠)发主播工资",7:"清空家族 / 主播K豆"}
        module = self.__class__.__name__
        msg = sys._getframe().f_code.co_name
        try:
            # 秀币消耗支出流水
            sql = '''select a.*, b.description from (
            select ntype, sum(cons_amount) as consume, sum(incm_amount) as income from melotlog.hist_user_showmoney_inout
            where dtime>='%s' and dtime<'%s' and
            (ntype not in (2,28,29,17) or ntype is null) group by ntype) a left join
            melotlog.conf_inout_ntype b on a.ntype = b.ntype ORDER BY a.ntype
            ''' % (self.first, self.last)
            print sql
            self.melotlog_cursor.execute(sql)
            res = self.melotlog_cursor.fetchall()
            arrs1 = []
            arrs2 = []
            for line in res:
                desc = line[3] if line[3]!=None else '其他'
                data1 = (self.first, 1, 1, line[0], desc, line[2])
                arrs1.append(data1)
                data2 = (self.first, 1, 2, line[0], desc, line[1])
                arrs2.append(data2)
            # 充值秀币流水
            mini_sql='''select sum(mimoney+extramimoney) from melotpay.recharging_record a
            where a.affirmtime >= '%s' and a.affirmtime < '%s' and a.state = 1
            ''' % (self.first, self.last)
            print mini_sql
            self.melotpay_cursor.execute(mini_sql)
            mini_res = self.melotpay_cursor.fetchone()[0]
            data1 = (self.first, 1, 1, 2, '充值', mini_res)
            arrs1.append(data1)

            # 库存收入
            inventory_incs_sql='''select sum(a.income_num *b.sendprice) from melotlog.hist_inventory_income a,melotlog.gift_info b 
            where a.dtime>='%s' and a.dtime<'%s' and a.ivty_id=b.giftId
            ''' % (self.first, self.last)
            print inventory_incs_sql
            self.melotlog_cursor.execute(inventory_incs_sql)
            inventory_incs_res = self.melotlog_cursor.fetchone()[0]
            data1 = (self.first, 1, 1, -99, '库存', inventory_incs_res)
            arrs1.append(data1)

            # 库存消耗
            inventory_cons_sql='''select sum(a.cons_num *b.sendprice) from melotlog.hist_inventory_consume a,melotlog.gift_info b 
            where a.dtime>='%s' and a.dtime<'%s' and a.ivty_id=b.giftId
            ''' % (self.first, self.last)
            print inventory_cons_sql
            self.melotlog_cursor.execute(inventory_cons_sql)
            inventory_cons_res = self.melotlog_cursor.fetchone()[0]
            data2 = (self.first, 1, 2, -99, '库存', inventory_cons_res)
            arrs2.append(data2)

            #VIP消耗
            vip_sql = '''SELECT sum(cons_amount) FROM melotlog.hist_user_showmoney_inout WHERE ntype=17 AND dtime>='%s' AND dtime<'%s'
            ''' % (self.first, self.last)
            print vip_sql
            self.melotlog_cursor.execute(vip_sql)
            vip_res = self.melotlog_cursor.fetchone()[0]
            print vip_res
            data2 = (self.first, 1, 2, 17, '购买道具', vip_res)
            arrs2.append(data2)

            # 家族违规
            infraction_sql='''select type, sum(actor_kbi+family_kbi) as amts from kkcx.hist_actor_family_infraction 
            where dtime >= '%s' and dtime < '%s' group by type order by type
            ''' % (self.first, self.last)
            print infraction_sql
            self.pg_cursor.execute(infraction_sql)
            infraction_res = self.pg_cursor.fetchall()
            arrs4 = []
            for line in infraction_res:
                desc = '其他'
                if infraction_map.has_key(line[0]):
                    desc = infraction_map[line[0]]
                data4 = (self.first, 1, 4, line[0], desc, line[1])
                arrs4.append(data4)

            time1 = time.time()
            #钻石消耗
            game_type = {}
            type_sql = '''select ntype, description from kkgame.conf_gamemoney_inout_ntype'''
            print type_sql
            self.game_cursor.execute(type_sql)
            type_res = self.game_cursor.fetchall()
            for line in type_res:
                game_type[line[0]]=line[1]
            localtime1 = time.strptime(self.first, '%Y%m%d')
            localtime2 = time.strptime('20180401', '%Y%m%d')
            if time.mktime(localtime1) >= time.mktime(localtime2):
                game_sql = ''' SELECT ntype, SUM(cons_amount), SUM(incm_amount) FROM hist_user_gamemoney_inout
                WHERE dtime >= STR_TO_DATE('%s', '%%Y%%m%%d')
                AND dtime < DATE_ADD(STR_TO_DATE('%s', '%%Y%%m%%d'), INTERVAL 1 DAY)
                GROUP BY ntype ''' % (self.first, self.last)
            else:
                game_sql='''SELECT ntype, SUM(cons_amount), SUM(incm_amount) FROM (
                    SELECT ntype, cons_amount, incm_amount FROM hist_user_gamemoney_inout_bak_180315
                    WHERE dtime >= STR_TO_DATE('%s', '%%Y%%m%%d')
                    AND dtime < DATE_ADD(STR_TO_DATE('%s', '%%Y%%m%%d'), INTERVAL 1 DAY)
                    UNION ALL
                    SELECT ntype, cons_amount, incm_amount FROM hist_user_gamemoney_inout
                    WHERE dtime >= STR_TO_DATE('%s', '%%Y%%m%%d')
                    AND dtime < DATE_ADD(STR_TO_DATE('%s', '%%Y%%m%%d'), INTERVAL 1 DAY)
                    AND histid NOT IN (
                    SELECT histid FROM hist_user_gamemoney_inout WHERE dtime < STR_TO_DATE('2018-03-15 17:45:02', '%%Y-%%m-%%d %%H:%%i:%%s')
                    AND histid > 1000 )
                ) a GROUP BY ntype''' % (self.first, self.last, self.first, self.last)
            print game_sql
            self.game_cursor.execute(game_sql)
            game_res = self.game_cursor.fetchall()
            arrs5 = []
            arrs6 = []
            print len(game_res)
            for line in game_res:
                desc = '其他'
                if game_type.has_key(line[0]):
                    desc = game_type[line[0]]
                if line[2]>0:
                    data5 = (self.first, 1, 5, line[0], desc, line[2])
                    arrs5.append(data5)
                if line[1]>0:
                    data6 = (self.first, 1, 6, line[0], desc, line[1])
                    arrs6.append(data6)

            time2 = time.time()
            print "live_date=", time2 - time1
            print arrs2
            keys = ['dt', 'appid', 'data_type', 'ntype', 'type_desc', 'amounts']
            self.dao.insertBeforeDelete("finance_month_category_stats", " where dt = " + self.first + " and appid=1 and data_type=1", keys, arrs1)
            self.dao.insertBeforeDelete("finance_month_category_stats", " where dt = " + self.first + " and appid=1 and data_type=2", keys, arrs2)
            self.dao.insertBeforeDelete("finance_month_category_stats", " where dt = " + self.first + " and appid=1 and data_type=4", keys, arrs4)
            self.dao.insertBeforeDelete("finance_month_category_stats", " where dt = " + self.first + " and appid=1 and data_type=5", keys, arrs5)
            self.dao.insertBeforeDelete("finance_month_category_stats", " where dt = " + self.first + " and appid=1 and data_type=6", keys, arrs6)
            logger.genlog.debug('[%s]:[%s] execute sucess!' % (module, msg))
        except Exception:
            logger.genlog.error('[%s]:[%s] execute failed!' % (module, msg))
            logger.errlog.error(traceback.format_exc())

    def report(self):
        self.consume_amounts()

    def __del__(self):
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.melotlog_cursor:
            self.melotlog_cursor.close()
        if self.melotpay_cursor:
            self.melotpay_cursor.close()
        # if self.game_cursor:
        #     self.game_cursor.close()

def main():
    dttime = datetime.datetime.now().strftime("%Y%m%d")
    FinanceMonthCategoryStats(dttime).report()

    # begin = datetime.date(2017, 5, 1)
    # end = datetime.date(2017, 9, 21)
    # d = begin
    # delta = datetime.timedelta(days=1)
    # while d <= end:
    #     str = d.strftime("%Y%m%d")
    #     print str
    #     bangAssetsInout(str).report()
    #     d += delta

if __name__ == '__main__':
    main()
