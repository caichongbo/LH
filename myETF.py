import backtrader as bt
from getdata import getData as gd
import pandas as pd

# 获取数据
def getData(code, start, end):
    filename = code+".csv"
    print("./" + filename)
    # 已有数据文件，直接读取数据
    if os.path.exists("./" + filename):
        df = pd.read_csv(filename)
    else: # 没有数据文件，用tushare下载
        df = ts.get_k_data(code, autype = "qfq", start = start, end = end)
        df.to_csv(filename)
    df.index = pd.to_datetime(df.date)
    df['openinterest']=0
    df=df[['open','high','low','close','volume','openinterest']]
    return df


class TestStrategy(bt.Strategy):
    """
    继承并构建自己的bt策略
    """
    def log(self, txt, dt=None, doprint=True):
        ''' 日志函数，用于统一输出日志格式 '''
        # 记录策略的执行日志
        if doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))



    def __init__(self):

        # 初始化相关数据
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 五日移动平均线
        self.sma5 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=5)
        # 十日移动平均线
        self.sma10 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=10)
        # 二十日移动平均线
        self.sma20 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=20)



    def notify_order(self, order):
        """
        订单状态处理

        Arguments:
            order {object} -- 订单状态
        """
        if order.status in [order.Submitted, order.Accepted]:
            # 如订单已被处理，则不用做任何事情
            return

        # 检查一个订单是否完成
        # 注意: 当资金不足时，broker会拒绝订单
        if order.status in [order.Completed]:
            if order.isbuy():
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
                self.log('已买入 %.2f，买入金额 %.2f' % (order.executed.price,order.executed.value))

            elif order.issell():
                self.log('已卖出 %.2f，卖出金额 %.2f' % (order.executed.price,order.executed.value))

            # 记录当前交易数量
            self.bar_executed = len(self)

        # 订单因为缺少资金之类的原因被拒绝执行
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print('因为缺少资金之类的原因被拒绝执行：Order Canceled/Margin/Rejected 可用资金:'+ str(cerebro.broker.getvalue())+ '  订单金额：'+str(order.executed.price))

        # 订单状态处理完成，设为空
        self.order = None

    def notify_trade(self, trade):
        """
        交易成果

        Arguments:
            trade {object} -- 交易状态
        """
        if not trade.isclosed:
            return

        # 显示交易的毛利率和净利润
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        ''' 下一次执行 '''

        # 记录收盘价
        # self.log('Close, %.2f' % self.dataclose[0])

        # 是否正在下单，如果是的话不能提交第二次订单
        if self.order:
            return

        #买卖策略
        # 是否已经买入
        # if not self.position:
        #     # 还没买，如果 MA5 > MA10 说明涨势，买入
        #     if self.sma5[0] > self.sma10[0]:
        #         self.order = self.buy()
        # else:
        #     # 已经买了，如果 MA5 < MA10 ，说明跌势，卖出
        #     if self.sma5[0] < self.sma10[0]:
        #         self.order = self.sell()

        self.log( '当天收盘：'+ str(self.dataclose[0]) + '当天5日均线：'+ str(self.sma5[0]))

        if not self.position:  #如果没有持仓
            if self.dataclose > self.sma5:
                if self.sma5 > self.sma10:
                #     if self.sma10 > self.sma20:
                        self.order = self.buy()   # 跟踪订单避免重复
        else:
            if self.dataclose < self.sma5:
                if self.sma5 < self.sma10:
                #     if self.sma10 < self.sma20:
                        self.order = self.sell()   # 跟踪订单避免重复




    def stop(self):
        self.log(u'(最后结果) Ending Value %.2f' %
                 (self.broker.getvalue()), doprint=True)
        # self.log("最大回撤:-%.2f%%" % self.stats.drawdown.maxdrawdown[-1], doprint=True)
        self.log("夏普比例:", results[0].analyzers.sharpe.get_analysis())




if __name__ == '__main__':

    # 初始化模型
    cerebro = bt.Cerebro()

    # 构建策略
    strats = cerebro.addstrategy(TestStrategy)
    # 每次买100股
    # cerebro.addsizer(bt.sizers.FixedSize, stake=500000)
    cerebro.addsizer(bt.sizers.PercentSizer,percents =95)

    # 加载数据到模型中
    sql =  "SELECT d.date_time,d.nav,d.nav,d.nav,d.nav,d.nav FROM fund_nav d WHERE d.fund_code =  '510300' order by d.date_time asc"
    #sql =  "SELECT d.trade_date,d.close,d.high,d.low,d.open,d.vol FROM stock_daily d WHERE d.ts_code =  '600036.SH' order by d.trade_date asc"

    sqlresult = gd.getData.select_form(sql)
    result = pd.DataFrame(sqlresult, columns=['datetime', 'close', 'high', 'low', 'open', 'volume'])
    result.reset_index(inplace=True)
    result['datetime'] = pd.to_datetime(result['datetime'],format='%Y-%m-%d')
    result.set_index('datetime',inplace=True)
    result.dropna(inplace=True)

    #result.loc[0]=['date','close','high','low','open','vol']
    data = bt.feeds.PandasData(dataname=result)
        # fromdate=datetime.datetime(2019, 1, 1),
        # todate=datetime.datetime(2020, 10, 31),
        # nullvalue=0.0,
        # dtformat=('%Y/%m/%d'),
        # datetime=0,
        # open=4,
        # high=2,
        # low=3,
        # close=1,
        # volume=5,
        # openinterest=-1
        # predict=5
    # )


    cerebro.adddata(data)

    # 设定初始资金和佣金
    cerebro.broker.setcash(1000000.0)
    cerebro.broker.setcommission(0.0002)

    # 分析框架夏普比率和回撤
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')


    # 策略执行前的资金
    print('启动资金: %.2f' % cerebro.broker.getvalue())

    # 策略执行
    cerebro.run()
    print('启动后资金: %.2f' % cerebro.broker.getvalue())
    # print(cerebro.)
    cerebro.plot()

