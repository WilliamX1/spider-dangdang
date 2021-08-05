# MySQL table wants
# id num timestamp buyer goods seller
# data repeatable

import time, random
import pymysql
from pymysql import cursors
import logging
from configuration import db_user, db_password, generateRandomTimestamp, connectToMySQL

size = 1000000
isAddTo = False

def generateWants():
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
    if isAddTo is False: cursor.execute('DELETE FROM wants')
    else: 
        # 获取wants_id最大值
        cursor.execute('SELECT MAX(id) FROM wants')
        max_id_str = str(cursor.fetchone())[1:-2]
        max_wants_id = int(max_id_str) if max_id_str != 'None' else 0
        start_id = max_wants_id + 1
    # 使用的表名
    wantstable = 'wants'
    # id num timestamp buyer goods seller
    wantsql = ''.join(['insert into ', wantstable, ' values(%s,%s,%s,%s,%s,%s)'])
    
    logging.info('--------Start insert data into MySQL table wants--------')
    for i in range(start_id, start_id + size):
        id = i
        num = random.randint(1, 4)
        timestamp = generateRandomTimestamp()
        goods = random.randint(min_goods_id, max_goods_id)
        buyer = random.randint(min_user_id, max_user_id)
        seller = random.randint(min_user_id, max_user_id)
        # 确保buyer和seller不一样
        while (seller == buyer): seller = random.randint(min_user_id, max_user_id)
        # 准备参数
        args = [id, num, timestamp, buyer, goods, seller]
        try:
            cursor.execute(wantsql, args)
            logging.info(args)
        except Exception as e:
            logging.exception(e)
            connection.rollback()
    connection.commit()
    cursor.close()
    connection.close()
    logging.info('--------Finish load table wants into MySQL--------')

logging.basicConfig(format = '%(asctime)s - %(pathname)s[line:%(lineno)d]\n%(levelname)s: %(message)s',
                    level = logging.DEBUG)
generateWants()