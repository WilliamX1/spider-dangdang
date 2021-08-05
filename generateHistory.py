# MySQL table history
# id time goods user
# data repeatable

import time, random
import pymysql
from pymysql import cursors
import logging
from configuration import db_user, db_password, generateRandomTimestamp, connectToMySQL

size = 400000
isAddTo = True
table = 'history'

def getBasicInfo(cursor):
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
    return min_user_id, max_user_id, min_goods_id, max_goods_id

def generateOrders():
    cursor, connection = connectToMySQL()
    min_user_id, max_user_id, min_goods_id, max_goods_id = getBasicInfo(cursor)
    
    start_id = 1
    if isAddTo is False: cursor.execute(''.join(['DELETE FROM ', table]))
    else:
        # 获取id最大值
        cursor.execute(''.join(['SELECT MAX(id) FROM ', table]))
        max_id_str = str(cursor.fetchone())[1:-2]
        max_id = int(max_id_str) if max_id_str != 'None' else 0
        start_id = max_id + 1
    # id time goods user  
    sql = ''.join(['insert into ', table, ' values(%s,%s,%s,%s)'])
    
    logging.info(''.join(['--------Start insert data into MySQL table ', table, '--------']))
    for i in range(start_id, start_id + size):
        id = i
        time = generateRandomTimestamp()
        goods = random.randint(min_goods_id, max_goods_id)
        user = random.randint(min_user_id, max_user_id)

        # 准备参数
        args = [id, time, goods, user]
        try:
            cursor.execute(sql, args)
            logging.info(args)
        except Exception as e:
            logging.exception(e)
            connection.rollback()
    connection.commit()
    cursor.close()
    connection.close()
    logging.info(''.join(['--------Finish load table ', table, ' into MySQL--------']))

logging.basicConfig(format = '%(asctime)s - %(pathname)s[line:%(lineno)d]\n%(levelname)s: %(message)s',
                    level = logging.DEBUG)
generateOrders()