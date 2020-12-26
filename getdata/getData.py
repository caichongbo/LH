
import pymysql


class getData():

    def select_form(sql):
        # 打开数据库连接
        db = pymysql.connect("localhost", "root", "123456", "stock")

        # 使用cursor()方法获取操作游标
        cursor = db.cursor()

        # SQL 查询语句
        #sql = """SELECT d.trade_date,d.close,d.high,d.low,d.open,d.vol FROM stock_daily d WHERE d.ts_code =  %s"""%(3)

        try:
            # 执行SQL语句
            cursor.execute(sql)
            # 获取所有记录列表
            results = cursor.fetchall()

        except Exception as e:
            print("查询出错：case%s"%e)

        finally:
            # 关闭游标连接
            cursor.close()
            # 关闭数据库连接
            db.close()

        return results