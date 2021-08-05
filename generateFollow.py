# MySQL table history
# id follower user
# data unrepeatable

import time, random
import pymysql
from pymysql import cursors
import logging
from configuration import db_user, db_password, generateRandomTimestamp, connectToMySQL

size = 50000
isAddTo = True
table = 'follow'

def getBasicInfo(cursor):
    # 获取用户id最大值
    cursor.execute('SELECT MAX(user_id) FROM user')
    max_user_id = int(str(cursor.fetchone())[1:-2])
    # 获取用户id最小值
    cursor.execute('SELECT MIN(user_id) FROM user')
    min_user_id = int(str(cursor.fetchone())[1:-2])
    
    print('min_user_id : {} max_user_id : {}'
          .format(min_user_id, max_user_id))
    return min_user_id, max_user_id

def generateOrders():
    cursor, connection = connectToMySQL()
    min_user_id, max_user_id= getBasicInfo(cursor)
    
    start_id = 1
    if isAddTo is False: cursor.execute(''.join(['DELETE FROM ', table]))
    else:
        # 获取id最大值
        cursor.execute(''.join(['SELECT MAX(id) FROM ', table]))
        max_id_str = str(cursor.fetchone())[1:-2]
        max_id = int(max_id_str) if max_id_str != 'None' else 0 
        start_id = max_id + 1
    # id time goods user  
    sql = ''.join(['insert into ', table, ' values(%s,%s,%s)'])
    
    logging.info(''.join(['--------Start insert data into MySQL table ', table, '--------']))
    unrepeatable = {}
    for i in range(start_id, start_id + size):
        id = i
        follower = random.randint(min_user_id, max_user_id)
        user = random.randint(min_user_id, max_user_id)
        if unrepeatable.get(''.join([str(follower), str(user)])) == None: unrepeatable[''.join([str(follower), str(user)])] = True
        else: 
            while (unrepeatable.get(''.join([str(follower), str(user)])) != None):
                follower = random.randint(min_user_id, max_user_id)
                user = random.randint(min_user_id, max_user_id)
        # 准备参数
        args = [id, follower, user]
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