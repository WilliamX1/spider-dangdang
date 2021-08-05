import base64, requests
import hashlib
from posixpath import join
import pymysql, csv, codecs
import time, random
import os

filename = 'dangdang.csv'
file_encoding = 'utf-8'
db_user = 'root'
db_password = '200176'

# 随机生成时间戳
def generateRandomTimestamp():    
    start_time=(2012,1,1,0,0,0,0,0,0)              #设置开始日期时间元组(1976-01-01 00：00：00)
    end_time=(2021,8,3,23,59,59,0,0,0)    #设置结束日期时间元组(1990-12-31 23：59：59)
    start_timestamp = time.mktime(start_time)
    end_timestamp = time.mktime(end_time)
    t = random.randint(start_timestamp, end_timestamp)    #在开始和结束时间戳中随机取出一个
    date_touple = time.localtime(t)          #将时间戳生成时间元组
    date = time.strftime('%Y-%m-%d %H:%M:%S', date_touple)   #将时间元组转成格式化字符串（1976-05-21）
    return date
# 处理店铺和user对应,
def handleUser():
    with codecs.open(filename = filename, mode = 'r', encoding = file_encoding) as file:
        items = tuple(csv.reader(file))
        dict, item_table, user_table = {}, [], []
        user, m, n = 1, len(items), len(items[0])
        for i in range(1, m):
            item = {}
            for j in range(0, n): item[items[0][j]] = items[i][j]
            if dict.get(item['shop']) is None: 
                dict[item['shop']] = item['user'] = user
                user_table.append({'username': item['shop'], 'user_id': user})
                user = user + 1
            else: item['user'] = dict[item['shop']]
            item_table.append(item)
    return item_table, user_table
# load .csv data into mysql table goods
def loadDataIntoMySQL():
    connection = pymysql.connect(host = 'localhost', #host属性
                            user = db_user, #用户名 
                            password = db_password,  #此处填登录数据库的密码
                            db = 'mysql', #数据库名
                            )
    # 创建光标对象，一个连接可以有很多光标，一个光标跟踪一种数据状态。
    # 光标对象作用是：创建、删除、写入、查询等等
    cursor = connection.cursor()
    # 使用数据库passthemon
    cursor.execute('USE passthemon')
    # 处理user和店铺
    items, users = handleUser()
    # 插入数据库user表
    email_unique, phone_unique = {}, {}
    # 使用的表名字
    usertable = 'user'
    # 如果存在数据则删除
    cursor.execute('DELETE FROM goods')
    cursor.execute('DELETE FROM user')
    # user_id credit email gender image password phone refresh_token state username
    usersql = ''.join(['insert into ', usertable, ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'])
    for user in users:
        print(user)
        args = []
        # user_id
        args.append(user['user_id'])
        # credit
        args.append(random.randint(2, 5)) 
        # email
        email = abs(hash(user['username']))
        if email_unique.get(email) is not None:
            while (True):
                email = abs(hash(email))
                if email_unique.get(email) is None: break
        email_unique[email] = True
        args.append(''.join([str(email), '@sjtu.edu.cn']))
        # gender
        args.append(random.randint(1, 3))
        # image
        args.append(pymysql.NULL)
        # password
        args.append(hashlib.md5(b'123456').hexdigest()) 
        # phone
        phone = str(email)[0: 11]
        if phone_unique.get(phone) is not None:
            while (True):
                phone = hash(phone)
                if phone_unique.get(phone) is None: break
        phone_unique[phone] = True
        args.append(phone)
        # refresh_token
        args.append(pymysql.NULL)
        # state
        args.append(1)
        # username
        args.append(user['username'])
        try:
            cursor.execute(usersql, args)
            print('-----------------------')
        except Exception as e:
            print(e)
            connection.rollback()

    print("Start load table goods into MySQL, a lot of time, please wait...")
    # 使用的表名字
    goodstable = 'goods'
    # goods_id description inventory name price state upload_time user
    goodssql = ''.join(['insert into ', goodstable, ' values(%s,%s,%s,%s,%s,%s,%s,%s)'])
    for item in items:
        print(item)
        args = []
        args.append(item['goods_id'])
        args.append(item['description'])
        args.append(item['inventory'])
        args.append(item['name'])
        args.append(item['price'])
        args.append(item['state'])
        args.append(item['upload_time'])
        args.append(item['user'])
        try:
            cursor.execute(goodssql, args)
            print('-----------------------')
        except Exception as e:
            print(e)
            connection.rollback()
    connection.commit()
    cursor.close()
    connection.close()
    print("~~Finish load table goods into MySQL~~")

loadDataIntoMySQL()