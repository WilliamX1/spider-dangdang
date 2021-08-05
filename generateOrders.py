# MySQL table orders
# order_id comment num rating saleroom status timestamp buyer goods seller
# data repeatable
import time, random
import pymysql
from pymysql import cursors
import logging
from configuration import db_user, db_password, generateRandomTimestamp, connectToMySQL

db_user = 'root'
db_password = '200176'
size = 100000
isAddTo = False

def generateOrders():
    cursor, connection = connectToMySQL()
    # 获取用户id最大值
    cursor.execute('SELECT MAX(user_id) FROM user')
    max_user_id = int(str(cursor.fetchone())[1:-2])
    # 获取用户id最小值
    cursor.execute('SELECT MIN(user_id) FROM user')
    min_user_id = int(str(cursor.fetchone())[1:-2])
    # 获取商品id最大值
    cursor.execute('SELECT MAX(goods_id) FROM goods')
    max_goods_id = int(str(cursor.fetchone())[1:-2])
    # 获取商品id最小值
    cursor.execute('SELECT MIN(goods_id) FROM goods')
    min_goods_id = int(str(cursor.fetchone())[1:-2])
    
    print('min_user_id : {} max_user_id : {} min_goods_id : {} max_goods_id : {}'
          .format(min_user_id, max_user_id, min_goods_id, max_goods_id))
    
    start_id = 1
    if isAddTo is False: cursor.execute('DELETE FROM orders')
    else:
        # 获取order_id最大值
        cursor.execute('SELECT MAX(order_id) FROM orders')
        max_id_str = str(cursor.fetchone())[1:-2]
        max_order_id = int(max_id_str) if max_id_str != 'None' else 0
        start_id = max_order_id + 1
    # 使用的表名
    orderstable = 'orders'
    # order_id comment num rating saleroom status timestamp buyer goods seller  
    ordersql = ''.join(['insert into ', orderstable, ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'])
    
    # comment template
    comment_template = ['真好看', '真心好用', '很喜欢, 下次还会来', '牛啊牛啊', '这家店真良心', '商品很不错']
    logging.info('--------Start insert data into MySQL table orders--------')
    for i in range(start_id, start_id + size):
        order_id = i
        comment = random.sample(comment_template, 1)
        num = random.randint(1, 4)
        rating = random.randint(2, 5)
        status = 1
        timestamp = generateRandomTimestamp()
        goods = random.randint(min_goods_id, max_goods_id)
        buyer = random.randint(min_user_id, max_user_id)
        seller = random.randint(min_user_id, max_user_id)
        # 确保buyer和seller不一样
        while (seller == buyer): seller = random.randint(min_user_id, max_user_id)
        # 查询价格
        cursor.execute(''.join(['SELECT price FROM goods where goods_id = ', str(goods)]))
        saleroom_str = str(cursor.fetchone())
        first, second = saleroom_str.index('\'') + 1, saleroom_str.rindex('\'')
        saleroom = float(saleroom_str[first:second])
        # 准备参数
        args = [order_id, comment, num, rating, saleroom, status, timestamp, buyer, goods, seller]
        try:
            cursor.execute(ordersql, args)
            logging.info(args)
        except Exception as e:
            logging.exception(e)
            connection.rollback()
    connection.commit()
    cursor.close()
    connection.close()
    logging.info('--------Finish load table orders into MySQL--------')

logging.basicConfig(format = '%(asctime)s - %(pathname)s[line:%(lineno)d]\n%(levelname)s: %(message)s',
                    level = logging.DEBUG)
generateOrders()