# -*- coding: utf-8 -*-#

#-------------------------------------------------------------------------------
# PROJECT_NAME GetstockTickData
# Name:         getETFStockTickers
# Description:  
# Author:       lixin
# Date:         2020-12-23
#-------------------------------------------------------------------------------


import requests
from bs4 import BeautifulSoup
import time
import random
import pymysql
import os
import pandas as pd
import datetime
import re
from sqlalchemy import create_engine


"""
use invest ;
DROP TABLE IF  EXISTS fund_info ;
CREATE TABLE IF NOT EXISTS `fund_info` (
  `fund_code` varchar(255) NOT NULL COMMENT '基金代码',
  `fund_name` varchar(255) DEFAULT NULL COMMENT '基金全称',
  `fund_abbr_name` varchar(255) DEFAULT NULL COMMENT '基金简称',
  `fund_type` varchar(255) DEFAULT NULL COMMENT '基金类型',
  `issue_date` varchar(255) DEFAULT NULL COMMENT '发行日期',
  `establish_date` varchar(255) DEFAULT NULL COMMENT '成立日期',
  `establish_scale` varchar(255) DEFAULT NULL COMMENT '成立日期规模',
  `asset_value` varchar(255) DEFAULT NULL COMMENT '最新资产规模',
  `asset_value_date` varchar(255) DEFAULT NULL COMMENT '最新资产规模日期',
  `units` varchar(255) DEFAULT NULL COMMENT '最新份额规模',
  `units_date` varchar(255) DEFAULT NULL COMMENT '最新份额规模',
  `fund_manager` varchar(255) DEFAULT NULL COMMENT '基金管理人',
  `fund_trustee` varchar(255) DEFAULT NULL COMMENT '基金托管人',
  `funder` varchar(255) DEFAULT NULL COMMENT '基金经理人',
  `total_div` varchar(255) DEFAULT NULL COMMENT '成立来分红',
  `mgt_fee` varchar(255) DEFAULT NULL COMMENT '管理费率',
  `trust_fee` varchar(255) DEFAULT NULL COMMENT '托管费率',
  `sale_fee` varchar(255) DEFAULT NULL COMMENT '销售服务费率',
  `buy_fee` varchar(255) DEFAULT NULL COMMENT '最高认购费率',
  `buy_fee2` varchar(255) DEFAULT NULL COMMENT '最高申购费率',
  `benchmark` varchar(1000) DEFAULT NULL COMMENT '业绩比较基准',
  `underlying` varchar(500) DEFAULT NULL COMMENT '跟踪标的',
  `data_source` varchar(255) DEFAULT 'eastmoney' COMMENT '数据来源',
  `created_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_by` varchar(255) DEFAULT 'eastmoney' COMMENT '创建人',
  `updated_by` varchar(255) DEFAULT 'eastmoney' COMMENT '更新人',
  PRIMARY KEY (`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金基本信息表';
 
 
DROP TABLE IF  EXISTS fund_nav ;
CREATE TABLE IF NOT EXISTS `fund_nav` (
  `date_time` varchar(255) NOT NULL,
  `nav` float(15,8) DEFAULT NULL,
  `add_nav` float(15,8) DEFAULT NULL,
  `nav_chg_rate` varchar(255) DEFAULT NULL,
  `buy_state` varchar(255) DEFAULT NULL,
  `sell_state` varchar(255) DEFAULT NULL,
  `div` varchar(255) DEFAULT NULL,
  `fund_code` varchar(255) NOT NULL,
  `created_date` datetime DEFAULT NULL,
  `updated_date` datetime DEFAULT NULL,
   PRIMARY KEY (`date_time`,`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 
DROP TABLE IF  EXISTS fund_nav_currency ;
CREATE TABLE IF NOT EXISTS  `fund_nav_currency` (
  `the_date` varchar(255) NOT NULL,
  `fund_code` varchar(255) NOT NULL,
  `profit_per_units` float(15,8) DEFAULT NULL,
  `profit_rate` varchar(255) DEFAULT NULL,
  `buy_state` varchar(255) DEFAULT NULL,
  `sell_state` varchar(255) DEFAULT NULL,
  `div` varchar(255) DEFAULT NULL,
 
  `created_date` datetime DEFAULT NULL,
  `updated_date` datetime DEFAULT NULL,
  PRIMARY KEY (`the_date`,`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=512800&page=1&per=841
http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=512800&page=1&per=1

"""


def randHeader():
    '''
    随机生成User-Agent
    :return:
    '''
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = ['Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                       'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                       'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11',
                       'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0'
                       ]
    result = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': head_user_agent[random.randrange(0, len(head_user_agent))]
    }
    return result


def getCurrentTime():
    # 获取当前时间
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))


def getURL(url, params=None, tries_num=5, sleep_time=0, time_out=10, max_retry=5):
    '''
       这里重写get函数，主要是为了实现网络中断后自动重连，同时为了兼容各种网站不同的反爬策略及，通过sleep时间和timeout动态调整来测试合适的网络连接参数；
       通过isproxy 来控制是否使用代理，以支持一些在内网办公的同学
    :param url:
    :param tries_num:  重试次数
    :param sleep_time: 休眠时间
    :param time_out: 连接超时参数
    :param max_retry: 最大重试次数，仅仅是为了递归使用
    :return: response
    '''
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    try:
        res = requests.Session()
        if isproxy == 1:
            res = requests.get(url, params=params,headers=header, timeout=time_out, proxies=proxy)
        else:
            res = requests.get(url, params=params,headers=header, timeout=time_out)
        res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
    except requests.RequestException as e:
        sleep_time_p = sleep_time_p + 10
        time_out_p = time_out_p + 10
        tries_num_p = tries_num_p - 1
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        if tries_num_p > 0:
            time.sleep(sleep_time_p)
            print(getCurrentTime(), url, 'URL Connection Error: 第', max_retry - tries_num_p, u'次 Retry Connection', e)
            return getURL(url, tries_num_p, sleep_time_p, time_out_p, max_retry)
    return res


class PyMySQL:

    # 获取当前时间
    def getCurrentTime(self):
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    # 数据库初始化
    def _init_(self, host, user, passwd, db, port=3306, charset='utf8'):
        pymysql.install_as_MySQLdb()
        try:
            self.db = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=3306, charset='utf8')
            # self.db = pymysql.connect(ip, username, pwd, schema,port)
            self.db.ping(True)  # 使用mysql ping来检查连接,实现超时自动重新连接
            print(self.getCurrentTime(), u"MySQL DB Connect Success:", user + '@' + host + ':' + str(port) + '/' + db)
            self.cur = self.db.cursor()
        except  Exception as e:
            print(self.getCurrentTime(), u"MySQL DB Connect Error :%d: %s" % (e.args[0], e.args[1]))

    # 插入数据
    def insertData(self, table, my_dict):
        try:
            # self.db.set_character_set('utf8')
            cols = ', '.join(my_dict.keys())
            values = '"," '.join(my_dict.values())
            sql = "replace into %s (%s) values (%s)" % (table, cols, '"' + values + '"')
            # print (sql)
            try:
                result = self.cur.execute(sql)
                insert_id = self.db.insert_id()
                self.db.commit()
                # 判断是否执行成功
                if result:
                    # print (self.getCurrentTime(), u"Data Insert Sucess")
                    return insert_id
                else:
                    return 0
            except Exception as e:
                # 发生错误时回滚
                self.db.rollback()
                print(self.getCurrentTime(), u"Data Insert Failed: %s" % (e))
                return 0
        except Exception as e:
            print(self.getCurrentTime(), u"MySQLdb Error: %s" % (e))
            return 0

    def insertData(self, table, my_dict):
        try:
            # self.db.set_character_set('utf8')

            # df = pd.DataFrame(stockdata,
            #                   columns=['fund_code', 'dateime', 'nav', 'add_nav', 'nav_chg_rate', 'sell_state',
            #                            'div_record'])
            # engine = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + ':' + '3306/' + dbname)
            # tablename = 'updateData'
            # df.to_sql(
            #     name=tablename,
            #     con=engine,
            #     index=False,
            #     if_exists='append')

            cols = ', '.join(my_dict.keys())
            values = '"," '.join(my_dict.values())
            sql = "insert into %s (%s) values (%s)" % (table, cols, '"' + values + '"')
            print(sql)
            # print (sql)
            try:
                result = self.cur.execute(sql)
                insert_id = self.db.insert_id()
                self.db.commit()
                # 判断是否执行成功
                if result:
                    # print (self.getCurrentTime(), u"Data Insert Sucess")
                    return insert_id
                else:
                    return 0
            except Exception as e:
                # 发生错误时回滚
                self.db.rollback()
                print(self.getCurrentTime(), u"Data Insert Failed: %s" % (e))
                return 0
        except Exception as e:
            print(self.getCurrentTime(), u"MySQLdb Error: %s" % (e))
            return 0

    def GetNearestDate(self, code):
        print('开始获取数据库表内股票的最后交易日期')
        sql_s_dateTime = 'select * from fund_nav where fund_code=' + code + ' order by date_time desc limit 1'  # 取出数据表最后一行记录
        # lastrow = pd.read_sql(sql_s_dateTime, self.db)
        self.cur.execute(sql_s_dateTime)
        data = self.cur.fetchone()
        print(data)
        if data is None:
            dateTime = '1990-12-12'
        else:
            dateTime = data[0]


        # self.db.close()
        return dateTime

    def updateData(self, table, my_dict):
        try:
            # self.db.set_character_set('utf8')
            cols = ', '.join(my_dict.keys())
            values = '"," '.join(my_dict.values())
            fund_code = my_dict['fund_code']
            sql = "update %s (%s) values (%s) where fund_code = %s" % (table, cols, '"' + values + '"', fund_code)
            # print (sql)
            try:
                self.cur.execute(sql)
                self.db.insert_id()
                self.db.commit()
                # 判断是否执行成功
            except Exception as e:
                # 发生错误时回滚
                self.db.rollback()
                print(self.getCurrentTime(), u"Data Insert Failed: %s" % (e))
                return 0
        except Exception as e:
            print(self.getCurrentTime(), u"MySQLdb Error: %s" % (e))
            return 0

    def GetBaseNearestDate(self, code):
        print('开始获取数据库表内基金基本数据')
        sql_s_eft_base = 'select * from fund_info WHERE fund_code=' + code
        eft_base_data = pd.read_sql(sql_s_eft_base, self.db)
        if len(eft_base_data) < 1:
            return False
        else:
            return True


class FundSpiders():

    def getCurrentTime(self):
        # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))


    def getFundNav(self, fund_code, start, end):
        '''
        获取基金净值数据，因为基金列表中是所有基金代码，一般净值型基金和货币基金数据稍有差异，下面根据数据表格长度判断是一般基金还是货币基金，分别入库
        :param fund_code:
        :return:
        '''

        # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1
        '''
            #寿险获取单个基金的第一页数据，里面返回的apidata 接口中包含了记录数、分页及数据文件等
            #这里暂按照字符串解析方式获取，既然是标准API接口，应该可以通过更高效的方式批量获取全部净值数据，待后续研究。这里传入基金代码、分页页码和每页的记录数。先简单查询一次获取总的记录数，再一次性获取所有历史净值
            首次初始化完成后，如果后续每天更新或者定期更新，只要修改下每页返回的记录参数即可
        '''

         # 获取历史净值的总记录数
        records = 40
        result = []
        noData = "暂无数据"
        currentText = ""
        page = 1

        try:
            # 根据基金代码和总记录数，一次返回所有历史净值
            while noData not in currentText:
                fund_nav = 'http://fund.eastmoney.com/f10/F10DataApi.aspx'
                params = {'type': 'lsjz', 'code': fund_code, 'page': page, 'per': records, 'sdate': start, 'edate': end}
                res = getURL(fund_nav,params)
                currentText = res.text
                if noData not in currentText:
                    soup = BeautifulSoup(res.text, 'html.parser')

                    fund_code = fund_code
                    tables = soup.findAll('table')
                    tab = tables[0]
                    i = 0
                    # 先用本办法，解析表格，逐行逐单元格获取净值数据
                    trs = tab.findAll('tr')
                    for tr in trs:
                        # 跳过表头；获取净值、累计净值和日收益率数据 如果列数为7，可以判断为一般基金。当然也可以通过标题或者基金类型参数来判断，待后续优化
                        if tr.findAll('td') and len((tr.findAll('td'))) == 7:
                            i = i + 1
                            try:
                                datetime = (tr.select('td:nth-of-type(1)')[0].getText().strip())
                                nav = (tr.select('td:nth-of-type(2)')[0].getText().strip())
                                add_nav = (tr.select('td:nth-of-type(3)')[0].getText().strip())
                                nav_chg_rate = (tr.select('td:nth-of-type(4)')[0].getText().strip())
                                buy_state = (tr.select('td:nth-of-type(5)')[0].getText().strip())
                                sell_state = tr.select('td:nth-of-type(6)')[0].getText().strip()
                                div_record = tr.select('td:nth-of-type(7)')[0].getText().strip().strip('\'')
                                # print (self.getCurrentTime(),i,result['fund_code'],result['the_date'],result['nav'],result['add_nav'],result['nav_chg_rate'],result['buy_state'],result['sell_state'] )
                                result.append([datetime,nav,add_nav,nav_chg_rate,buy_state,sell_state,div_record,fund_code])
                            except  Exception as e:
                                print(self.getCurrentTime(), 'getFundNav3', fund_code, fund_nav, e)

                        else:
                            pass
                        # if i>=1:
                        #     break
                    # print(self.getCurrentTime(), 'getFundNav', result['fund_code'], '共', str(i) + '/' + str(records),
                    #       '行数保存成功')
                page += 1

            return result[::-1]
        except  Exception as e:
            print(self.getCurrentTime(), 'getFundNav2', fund_code, fund_nav, e)
            return None



def main():
    global mySQL, sleep_time, isproxy, proxy, header
    isproxy = 0
    mySQL = PyMySQL()
    fundSpiders = FundSpiders()
    mySQL._init_('127.0.0.1', 'root', '123456', 'stock')
    # mySQL.CreateTableETFBase()
    header = randHeader()
    # sleep_time = 0.1
    # # fundSpiders.getFundJbgk('000001')
    funds = ['510300','510500','512760','512000','512580','512170','512400','512010','510230','510150','512690',
            '512800','512660','515050','512880','159915','399001','510050']
    # funds = fundSpiders.getFundCodesFromCsv()
    # # fundSpiders.getFundManagers('000001')
    for fund in funds:

            # fundSpiders.getFundInfo(fund)
            # fundSpiders.getFundManagers(fund)
        today = datetime.datetime.now()
        endtime = today.strftime("%Y-%m-%d")
        start_time = mySQL.GetNearestDate(fund)
        result = fundSpiders.getFundNav(fund,start_time,endtime)
        print(result[:10])

        df = pd.DataFrame(result,columns=['date_time', 'nav', 'add_nav', 'nav_chg_rate', 'buy_state','sell_state','div_record','fund_code'])
        df.drop_duplicates(subset=['date_time'], keep='first')  #爬到重复数据的时候处理
        print(df.head())
        engine = create_engine('mysql+pymysql://' + 'root' + ':' + '123456' + '@' + '127.0.0.1' + ':' + '3306/' + 'stock')
        tablename = 'fund_nav'

        if result[0][0] == start_time:
            continue
        # try:
        df.to_sql(
            name=tablename,
            con=engine,
            index=False,
            if_exists='append')
        # except:
        #     pass
    mySQL.db.close()


    #
if __name__ == "__main__":
    main()

