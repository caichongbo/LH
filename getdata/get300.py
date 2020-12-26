import urllib.request
import datetime

import pandas as pd


def get_page(url):  #获取页面数据
    req=urllib.request.Request(url,headers={
        'Connection': 'Keep-Alive',
        'Accept': 'text/html, application/xhtml+xml, */*',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
    })
    opener=urllib.request.urlopen(req)
    page=opener.read()
    return page




def get_index_history_byNetease(index_id):
    """
    :param index_temp: for example, 'sh000001' 上证指数
    :return:
    """
    localtime = datetime.datetime.now().strftime("%Y%m%d")
    url='http://quotes.money.163.com/service/chddata.html?code=%s&start=20020104&end=%s&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'%(index_id,localtime)

    page=get_page(url).decode('gb2312') #该段获取原始数据
    page=page.split('\r\n')
    col_info=page[0].split(',')   #各列的含义
    index_data=page[1:]     #真正的数据

    #为了与现有的数据库对应，这里我还修改了列名，大家不改也没关系
    col_info[col_info.index('日期')]='  #该交易日期'
    col_info[col_info.index('股票代码')]='指数代码'
    col_info[col_info.index('名称')]='指数名称'
    col_info[col_info.index('成交金额')]='成交额'

    index_data=[x.replace("'",'') for x in index_data]  #去掉指数编号前的“'”
    index_data=[x.split(',') for x in index_data]

    index_data=index_data[0:index_data.__len__()-1]   #最后一行为空，需要去掉
    pos1=col_info.index('涨跌幅')
    pos2=col_info.index('涨跌额')
    posclose=col_info.index('收盘价')
    index_data[index_data.__len__()-1][pos1]=0      #最下面行涨跌额和涨跌幅为None改为0
    index_data[index_data.__len__()-1][pos2]=0
    for i in range(0,index_data.__len__()-1):       #这两列中有些值莫名其妙为None 现在补全
        if index_data[i][pos2]=='None':
            index_data[i][pos2]=float(index_data[i][posclose])-float(index_data[i+1][posclose])
        if index_data[i][pos1]=='None':
            index_data[i][pos1]=(float(index_data[i][posclose])-float(index_data[i+1][posclose]))/float(index_data[i+1][posclose])

    pd.DataFrame(index_data).to_csv("D:\LH\data\hs300.csv",index=False,sep=',')
    # print(col_info)

# 1399300

get_index_history_byNetease(1399300)