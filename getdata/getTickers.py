import requests, pymysql, re, datetime
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
}


def DownStockCode():
    print('开始获取上证、深证股票代码')
    count = 0
    stockcodeList=[]
    urlList = ['https://www.banban.cn/gupiao/list_sh.html','https://www.banban.cn/gupiao/list_sz.html']
    for url in urlList:
        res = requests.get(url=url,headers=headers)
        bs_res = BeautifulSoup(res.text,'html.parser')
        stocklist=bs_res.find('div',id='ctrlfscont').find_all('li')
        for stock in stocklist:
                    stockhref=stock.find('a')['href']
                    list_stockhref=stockhref.strip().split('/')
                    stock_code=list_stockhref[2]
                    stockcodeList.append(stock_code)
                    count += 1
                    print('当前已获取{}只股票代码'.format(count),end='\r')
    print('已获取所有上证、深证股票代码：{}个'.format(len(stockcodeList)))
    return stockcodeList


def DownStockZSCode():
    print('开始获取上证指数、深证指数代码')




def CreateDatabase(host,user,password,dbname):
    db = pymysql.connect(
        host = host,
        user = user,
        password = password,
        port = 3306
    )
    cursor = db.cursor()
    sql = 'create database if not exists '+ dbname
    cursor.execute(sql)
    db.close()
    print('检查/创建本地存储数据库-STOCK')

#根据股票代码，创建添加股票数据表，用于存储每只股票数据，table:s_600001
def CreateTable(host,user,password,dbname,codeList):
    print('开始检查/添加股票数据表')
    db = pymysql.connect(
        host = host,
        user = user,
        password = password,
        database = dbname,
        charset="utf8"
    )
    cursor = db.cursor()
    count = len(codeList)
    num =  0
    for code in codeList:
        num += 1
        sql = 'create table if not exists s_'+code+'(\
            stockName varchar(250) not null,\
            dateTime varchar(250) not null, \
            startPrice varchar(100), \
            maxPrice varchar(100), \
            minPrice varchar(100), \
            endPrice varchar(100), \
            diffPrice varchar(100), \
            diffPercent varchar(100), \
            turnoverAmount varchar(100), \
            amount varchar(100), \
            amplitude varchar(100), \
            turnoverPercent varchar(100)) DEFAULT CHARSET=utf8'
        cursor.execute(sql)
        print('检查/添加数据库表：s_{}，{}/{}'.format(code,num,count),end='\r')
    db.close()

    print('数据库表已检查/添加完毕！！')

#获取所有表名，遍历每个表并获取最新行情数据日期返回列表：[[股票代码，最新数据日期],['600001','2020-12-3'],['600002','NULL']]
def GetNearestDate(host,user,password,dbname):
    print('开始获取数据库表内股票的最后交易日期')
    count_empty = 0 #用于记录空表数量
    tablelist = [] #存储获取到的数据库表名
    nearestdatelist = []#存储股票代码和最新行情日期
    db = pymysql.connect(
        host = host,
        user = user,
        password = password,
        database = dbname,
        charset="utf8"
    )
    cursor = db.cursor()
    sql_showtables = 'show tables'
    cursor.execute(sql_showtables) #查询表名
    for i in cursor:
        tablelist.append(str(i)) #表名存储到列表
    for tablename in tablelist:
        code = re.sub('\D','',tablename)#取出表名中的数字，即股票代码code
        sql_s_dateTime = 'select * from s_'+code+' order by dateTime desc limit 1' #取出数据表最后一行记录
        lastrow = pd.read_sql(sql_s_dateTime,db)
        if lastrow.empty:
            dateTime = 'Null'
            count_empty += 1
        else:
            dateTime = lastrow['dateTime'][0]
        nearestdatelist.append([code,dateTime])
        print('已获取:{} 的最后交易日期为:{}  当前获取进度：{}/{}     '.format(code,dateTime,len(nearestdatelist),len(tablelist)), end='\r')
    db.close()
    print('个股最后交易日期获取完毕,共计{}只股票，其中需下载全部数据的股票共{}只'.format(len(nearestdatelist),count_empty))
    return nearestdatelist


#读取数据库表，根据读取结果更新下载股票数据— 交易日为NULL，下载全部数据；交易日为最新，跳过；交易日非最新交易日，更新数据
def UpdateAndDown(host,user,password,dbname,new_date):
    count_downall = 0 #记录需下载全部历史数据的个股数量
    count_update = 0 #记录需更新数据的个股数量
    count_all = 0 #记录已遍历数据库的个股数量
    stocklist = GetNearestDate(host,user,password,dbname)
    stocknum = len(stocklist)
    for stock in stocklist:
        count_all += 1
        print('股票数据爬取进度{}/{}  正在更新数据库表：s_{}'.format(count_all,stocknum,stock[0]),end='\r')
        if stock[1] == 'Null': #如果个股最近数据日期为空，则下载全部股票数据到数据库
            stockdata = DownAllData(stock[0])
            count_downall +=1
        elif stock[1] == new_date: #若个股最近数据日期是最新交易额日期，则跳过
            continue
        else:
            stockdata = UpdateData(stock[0],stock[1])#若个股最近数据日期不为空，则更新数据库数据
            count_update +=1
        df = pd.DataFrame(stockdata,columns=['stockName','dateTime','startPrice','maxPrice','minPrice','endPrice','diffPrice','diffPercent','turnoverAmount','amount','amplitude','turnoverPercent'])
        engine = create_engine('mysql+pymysql://'+user+':'+password+'@'+host+':'+'3306/'+dbname)
        tablename = 's_'+stock[0]
        df.to_sql(
            name = tablename,
            con = engine,
            index = False,
            if_exists = 'append')
    print('股票数据全部下载完毕！下载全部数据股票数：{}只,更新数据股票数：{}只'.format(count_downall,count_update))



def GetSeason(month):
    month = int(month)
    if month >= 1 and month <= 3:
        season = 1
    elif month >= 4 and month <= 6:
        season = 2
    elif month >= 7 and month <= 9:
        season = 3
    else:
        season = 4
    return season



#若是空表，下载该股全部历史行情数据,返回：stockName-股票名字,stockdate-股票历史数据列表
def DownAllData(code):
    yearlist = [] #用于存储个股有数据的年份
    pagelist = [] #用于存储构造好的待爬取页面链接
    stockdate = [] #用于存储爬取到的股票数据
    url = 'http://quotes.money.163.com/trade/lsjysj_'+code+'.html?'
    res = requests.get(url = url,headers = headers)
    bs_res = BeautifulSoup(res.text,'html.parser')
    stockName = bs_res.find('div',class_='stock_info').find('h1',class_='name').find('a').text
    item = bs_res.find('form',id = 'date').find_all('option') #获取股票有数据的年份和季度
    now_dateTime = datetime.datetime.now().date()#获取当前日期
    now_year = now_dateTime.year
    now_month = now_dateTime.month
    now_season = GetSeason(now_month)
    for i in item[:-4]: #仅取出年份存入列表
        yearlist.append(i.text)
    for year in yearlist: #构造待爬取页面链接
        if int(year) == now_year: #若为当前年，按实际所在季度来构造链接数量，考虑当前日期所在季度不一定是第4季度的情况
            for i in range(now_season):
                season = now_season - i
                url_page = 'http://quotes.money.163.com/trade/lsjysj_'+code+'.html?year='+str(year)+'&season='+str(season)
                pagelist.append(url_page)
        else:
            for s in range(4): #非当前年将构造全部季度链接
                url_page = 'http://quotes.money.163.com/trade/lsjysj_'+code+'.html?year='+str(year)+'&season='+str(4-s)
                pagelist.append(url_page)
    for page in pagelist:
        res = requests.get(url = page,headers = headers)
        bs_res = BeautifulSoup(res.text,'html.parser')
        pageinfo = bs_res.find('table',class_='table_bg001').find_all('tr')
        flag = 0
        for row in pageinfo:
            if flag:
                rowData = row.find_all('td') #提取每一行所有td标签内容
                rowData_List = [] #用于存储取出的td标签内容
                for td in rowData:
                    rowData_List.append(td.text)
                dateTime = rowData_List[0]#开盘日期
                startPrice = rowData_List[1]#开盘价
                maxPrice = rowData_List[2]#最高价
                minPrice = rowData_List[3]#最低价
                endPrice = rowData_List[4]#收盘价
                diffPrice = rowData_List[5]#涨跌额
                diffPercent = rowData_List[6]#涨跌幅
                turnoverAmount = rowData_List[7]#成交量
                amount = rowData_List[8]#成交额
                amplitude = rowData_List[9]#振幅
                turnoverPercent = rowData_List[10]#换手率
                stockdate.append([stockName,dateTime,startPrice,maxPrice,minPrice,endPrice,diffPrice,diffPercent,turnoverAmount,amount,amplitude,turnoverPercent])
            else:
                flag = 1
    stockdate.reverse() #将排列顺序倒置，旧在前、新在后
    return stockdate



#若表不为空，则根据最后一条记录的开盘日期更新至实际最新日期数据,返回:股票名-stockName，股票待更新数据-stockdate
def UpdateData(code,dateTime):
    stockdata = [] #用于存储爬取到的股票数据
    dateTime = datetime.datetime.strptime(dateTime,'%Y-%m-%d').date()
    nowTime = datetime.datetime.now().date()#获取当前日期、年、月、季
    now_year = nowTime.year
    now_month = nowTime.month
    now_season = GetSeason(now_month)
    y = now_year #用于下文构造链接时控制年份
    s = now_season #用于下文构造链接时控制季度
    flag = 1 #控制循环
    while flag: #构造链接、爬取数据
        url = 'http://quotes.money.163.com/trade/lsjysj_'+code+'.html?year='+str(y)+'&season='+str(s)
        res = requests.get(url = url,headers = headers)
        bs_res = BeautifulSoup(res.text,'html.parser')
        stockName = bs_res.find('div',class_='stock_info').find('h1',class_='name').find('a').text
        pageinfo = bs_res.find('table',class_='table_bg001').find_all('tr')
        f = 0 #控制跳过股票数据第一行表头
        for row in pageinfo:
            if f:
                rowData = row.find_all('td') #提取每一行所有td标签内容
                rowData_List = [] #用于存储取出的td标签内容
                for td in rowData:
                    rowData_List.append(td.text)
                dateTime_ = rowData_List[0]#开盘日期
                startPrice = rowData_List[1]#开盘价
                maxPrice = rowData_List[2]#最高价
                minPrice = rowData_List[3]#最低价
                endPrice = rowData_List[4]#收盘价
                diffPrice = rowData_List[5]#涨跌额
                diffPercent = rowData_List[6]#涨跌幅
                turnoverAmount = rowData_List[7]#成交量
                amount = rowData_List[8]#成交额
                amplitude = rowData_List[9]#振幅
                turnoverPercent = rowData_List[10]#换手率
                if dateTime < datetime.datetime.strptime(dateTime_,'%Y-%m-%d').date(): #仅提取参数日前之后的行数据
                    stockdata.append([stockName,dateTime_,startPrice,maxPrice,minPrice,endPrice,diffPrice,diffPercent,turnoverAmount,amount,amplitude,turnoverPercent])
                else:
                    flag = 0 #如果行数据日期等于参数日期，则跳出while
                    break
            else:
                f = 1
        s -= 1 #如果当前页数据日期均晚于参数日期，则季度向前推1，继续构造上一季度链接进行爬取
        if s == 0: #如果季度向前推到了0，则恢复为4，年度减1
            s = 4
            y -= 1
    stockdata.reverse() #顺序倒置，旧在前，新在后
    return stockdata


#根据股票代码，创建添加股票数据表，用于存储每只股票数据，table:s_600001
def CreateTable1(host,user,password,dbname,):
    print('开始检查/添加股票数据表')
    db = pymysql.connect(
        host = host,
        user = user,
        password = password,
        database = dbname,
        charset="utf8"
    )
    cursor = db.cursor()

    num =  0
    code = 'qqq12313123'
    sql = "create table if not exists s_" + code + "(\
               stockName varchar(250) not null comment '1231',\
               dateTime varchar(250) not null, \
               startPrice varchar(100), \
               maxPrice varchar(100), \
               minPrice varchar(100), \
               endPrice varchar(100), \
               diffPrice varchar(100), \
               diffPercent varchar(100), \
               turnoverAmount varchar(100), \
               amount varchar(100), \
               amplitude varchar(100), \
               turnoverPercent varchar(100)) DEFAULT CHARSET=utf8"
    cursor.execute(sql)
    # print('检查/添加数据库表：s_{}，{}/{}'.format(code,num,count),end='\r')
    db.close()

    print('数据库表已检查/添加完毕！！')


if __name__ == '__main__':
    #获取股票代码
    stockcodeList = DownStockCode()
    #数据库检查
    CreateDatabase('127.0.0.1', 'root', '', 'Ticker')
    CreateTable('127.0.0.1', 'root', '', 'Ticker',stockcodeList)
    #爬取数据
    today = datetime.datetime.now()
    str_date = today.strftime("%Y-%m-%d")
    UpdateAndDown('127.0.0.1', 'root', '', 'Ticker',str_date)


