# coding:utf-8

import os, datetime, calendar
import sys,traceback
from common import logger
from common.utils import conn2PG_tshow, kkOption, conn2PG_melotlog, conn2PG_melotpay,conn2Impala
from common.mysql_dao_new import MysqlDao

class FinanceDayCategoryStats(object):
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

    def __init__(self, dttime):
        self.dttime = dttime

        self.pg_conn_tshow, self.pg_cursor_tshow = conn2PG_tshow()
        self.pg_conn_melotlog, self.pg_cursor_melotlog = conn2PG_melotlog()
        self.pg_conn_melotpay, self.pg_cursor_melotpay = conn2PG_melotpay()
        self.impalaconn, self.impalacur = conn2Impala()
        self.dao = MysqlDao()

    # 道具消耗统计
    def kk_prop_stats(self):
        module = self.__class__.__name__
        msg = sys._getframe().f_code.co_name
        try:
            self.today = datetime.datetime.strptime(self.dttime, "%Y%m%d").date()
            self.last_day = (self.today - datetime.timedelta(days=-1)).strftime("%Y%m%d")
            # VIP   13：自己购买VIP   14：他人赠送VIP 
            vip_sql = '''select describe,duration/30,count(*),sum(cons_amount) from melotlog.hist_user_resource_inout 
            WHERE accessmod in (13,14) and dtime>='%s' and dtime<'%s' GROUP BY describe,duration,cons_amount
            ''' % (self.dttime,self.last_day)
            print vip_sql
            self.pg_cursor_melotlog.execute(vip_sql)
            vip_res = self.pg_cursor_melotlog.fetchall()
            arrs1 = []
            for line in vip_res:
                data1 = (self.dttime, 1, 1, line[0], line[1], line[2], line[3])
                arrs1.append(data1)

            # 代理
            agent_sql = '''select cast(a.amount as int), count(recordid) as cnt, sum(amount) as amts
            from melotpay.recharging_record a where affirmtime>='%s' and affirmtime<'%s' and state=1 and amount>0 and paymentmode=71
            group by amount''' % (self.dttime, self.last_day)
            print agent_sql
            self.pg_cursor_melotpay.execute(agent_sql)
            agent_res = self.pg_cursor_melotpay.fetchall()
            arrs2 = []
            for line in agent_res:
                data2 = (self.dttime, 1, 2, line[0], 12, line[1], line[2])
                arrs2.append(data2)

            # 座驾 7月13  2:赠送座驾
            car_sql = '''select describe,duration/30,count(*),sum(cons_amount) from melotlog.hist_user_resource_inout 
            WHERE accessmod in (1,2) and dtime>='%s' and dtime<'%s' GROUP BY describe,duration,cons_amount''' % (self.dttime, self.last_day)
            print car_sql
            self.pg_cursor_melotlog.execute(car_sql)
            car_res = self.pg_cursor_melotlog.fetchall()
            arrs3 = []
            for line in car_res:
                data3 = (self.dttime, 1, 3, line[0], line[1], line[2], line[3])
                arrs3.append(data3)

            # 守护
            guard_sql = '''select guard_id, period, count(*) as cnts, sum(amount) as amts from tshow.hist_buy_guard
            where dtime>='%s' and dtime<'%s' group by guard_id, period''' % (self.dttime,self.last_day)
            self.pg_cursor_tshow.execute(guard_sql)
            guard_res = self.pg_cursor_tshow.fetchall()
            arrs4 = []
            for line in guard_res:
                data4 = (self.dttime, 1, 4, line[0], line[1], line[2], line[3])
                arrs4.append(data4)

            # 靓号
            pretty_sign = '''select ticket_type,period,count(distinct user_id),sum(amount) from tshow.hist_idticket_inout
            where dtime>='%s' and dtime<'%s' and amount>0 and present_type in(3,4,9) group by ticket_type,period''' % (self.dttime,self.last_day)
            print pretty_sign
            self.pg_cursor_tshow.execute(pretty_sign)
            pretty_sign_res = self.pg_cursor_tshow.fetchall()
            arrs6 = []
            for line in pretty_sign_res:
                data6 = (self.dttime, 1, 6, line[0], line[1], line[2], line[3])
                arrs6.append(data6)

            keys = ['dt', 'appid', 'data_type', 'ntype', 'valid_date', 'num', 'amounts']
            self.dao.insertBeforeDelete("finance_day_category_stats", " where dt = " + self.dttime + " and appid=1 and data_type=1", keys, arrs1)
            self.dao.insertBeforeDelete("finance_day_category_stats", " where dt = " + self.dttime + " and appid=1 and data_type=2", keys, arrs2)
            self.dao.insertBeforeDelete("finance_day_category_stats", " where dt = " + self.dttime + " and appid=1 and data_type=3", keys, arrs3)
            self.dao.insertBeforeDelete("finance_day_category_stats", " where dt = " + self.dttime + " and appid=1 and data_type=4", keys, arrs4)
            self.dao.insertBeforeDelete("finance_day_category_stats", " where dt = " + self.dttime + " and appid=1 and data_type=6", keys, arrs6)
            
            #守护追加现金消耗
            guard_sql_cash = '''SELECT a.productid,a.period,count(a.orderid) ,sum(a.amount) from
            (select CONCAT_WS('',substr(AFFIRMTIME,1,4),substr(AFFIRMTIME,6,2),substr(AFFIRMTIME,9,2)) as dt,c.period,n.amount,n.orderid,t.productid 
            from hist_buyproduct_recharge t, (select case substr(n.imei,9,18) when 'com.KKTV1.Bronze01' then 1 when 'com.KKTV1.Bronze02' then 2 
            when 'com.KKTV1.Silver01' then 5 when 'com.KKTV1.Silver02' then 6 end as price_id,n.*  from  dh_recharging_record n) n,conf_guard_price c   
            where t.orderid = n.orderid and t.type = 2 and n.STATE=1 and n.price_id=c.id
            and dt='%s')a group by a.productid,a.period 

            ''' % (self.dttime)
            print guard_sql_cash
            self.impalacur.execute(guard_sql_cash)
            res = self.impalacur.fetchall()
            print res
            data4_cash = []
            whe = {}
            whe['dt'] = self.dttime
            whe['data_type'] = 4
            whe['appid'] = 1
            user_keys = ['dt','cash_num','cash_amount']
            for line in res:
                whe['ntype'] = line[0]
                whe['valid_date'] = line[1]
                data4_cash=[self.dttime, line[2],line[3]]
                self.dao.insertBeforeQuery("finance_day_category_stats", whe, user_keys, data4_cash)

            #守护追加秀币消耗
            guard_sql_coin = '''SELECT product,count,count(histid),sum(cons_amount) 
            from dh_hist_user_showmoney_inout where ntype=20 and dt='%s' group by product,count
            ''' % (self.dttime)
            print guard_sql_coin
            self.impalacur.execute(guard_sql_coin)
            guard_res_coin = self.impalacur.fetchall()
            print guard_res_coin
            data4_coin = []
            whe = {}
            whe['dt'] = self.dttime
            whe['data_type'] = 4
            whe['appid'] = 1
            user_keys = ['dt','coin_num','coin_amount']
            for line in guard_res_coin:
                whe['ntype'] = line[0]
                whe['valid_date'] = line[1]
                data4_coin=[self.dttime, line[2],line[3]]
                self.dao.insertBeforeQuery("finance_day_category_stats", whe, user_keys, data4_coin)
	
            logger.genlog.debug('[%s]:[%s] execute sucess!' % (module, msg))
        except Exception:
            logger.genlog.error('[%s]:[%s] execute failed!' % (module, msg))
            logger.errlog.error(traceback.format_exc())

    def report(self):
        self.kk_prop_stats()

    def __del__(self):
        if self.pg_cursor_tshow:
            self.pg_cursor_tshow.close()
        if self.pg_cursor_melotpay:
            self.pg_cursor_melotpay.close()
        if self.pg_cursor_melotlog:
            self.pg_cursor_melotlog.close()

def main():
    dttime = kkOption()
    FinanceDayCategoryStats(dttime).report()

    # begin = datetime.date(2018,10,18)
    # end = datetime.date(2018,11,1)
    # d = begin
    # delta = datetime.timedelta(days=1)
    # while d <= end:
    #    str = d.strftime("%Y%m%d")
    #    print str
    #    FinanceDayCategoryStats(str).report()
    #    d += delta

if __name__ == '__main__':
    main()
