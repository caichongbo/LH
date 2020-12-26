import tushare as ts
import time
import numpy as np
import pymysql


def insert_db(sql,newdata):
    # 打开数据库连接
    db = pymysql.connect("localhost", "root", "123456", "stock", charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    try:
        # 执行sql语句
        for onedata in newdata:
            param = (onedata[0],onedata[1],float(onedata[2]),float(onedata[3]),float(onedata[4]),float(onedata[5]),float(onedata[6]),round(float(onedata[7]),3),float(onedata[8]),float(onedata[9]),float(onedata[10]))
            cursor.execute(sql, param)
        # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
        # Rollback in case there is any error
        db.rollback()

    # 关闭数据库连接
    db.close()


def get_pdata():  #获取数据
    #设置token
    ts.set_token('f7002abc88ceb1bb99d68e957c805cbccd4f553e02fa74b67295cad1')

    pro = ts.pro_api()


    # 查询当前所有正常上市交易的股票列表
    allst = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs')

    i = 0
    for st in allst.values:
        #print(st)
        # sql = "insert into stock_company(ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs) values(%s,%s,%f,%s,%s,%s,%s,%s,%s,%s,%s)"
        # param = (st[0], st[1], st[2], st[3], st[4], st[5], st[6], st[7], st[8], st[9], st[10])
        # insert_db(sql,param)

        # 取单个的前复权行情
        df = ts.pro_bar(ts_code=st[0], adj='qfq', start_date='19900101', end_date='20201231')
        i = i+1
        print(str(i) + '取单个的前复权行情开始:' + st[0]+ ' '+st[2] )

        #把为历史空的数据清除
        newdata =  np.array(df.values,dtype='str')
        newdata = newdata[newdata[:,2] != 'nan']

      #  for onedata in newdata:
            #每天数据
        sql = "insert into stock_daily(ts_code,trade_date,`open`,high,low,`close`,pre_close,`change`,pct_chg,vol,amount) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
       # param = (onedata[0],onedata[1],float(onedata[2]),float(onedata[3]),float(onedata[4]),float(onedata[5]),float(onedata[6]),round(float(onedata[7]),3),float(onedata[8]),float(onedata[9]),float(onedata[10]))
        insert_db(sql,newdata)


        print('取单个的前复权行情结束:'  + st[0]+ ' '+st[2] )

        #单个获取后暂停10秒
        time.sleep(1)  # 休眠10秒


get_pdata()