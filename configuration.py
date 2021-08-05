import time, random, pymysql

# 数据库信息
db_user = 'root'
db_password = '200176'

# 随机生成时间戳
def generateRandomTimestamp():    
    start_time=(2012,1,1,0,0,0,0,0,0)              # 设置开始日期时间元组(1976-01-01 00：00：00)
    end_time=(2021,8,3,23,59,59,0,0,0)    # 设置结束日期时间元组(1990-12-31 23：59：59)
    start_timestamp = time.mktime(start_time)
    end_timestamp = time.mktime(end_time)
    t = random.randint(start_timestamp, end_timestamp)    # 在开始和结束时间戳中随机取出一个
    date_touple = time.localtime(t)          # 将时间戳生成时间元组
    date = time.strftime('%Y-%m-%d %H:%M:%S', date_touple)   # 将时间元组转成格式化字符串（1976-05-21）
    return date

# 连接数据库
def connectToMySQL():
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
    return cursor, connection
