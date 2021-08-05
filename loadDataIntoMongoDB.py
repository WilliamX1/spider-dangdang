import logging
from pymysql import connect
import pymongo, csv, codecs, base64, requests, json
from bson import binary

filename = 'dangdang.csv'
file_encoding = 'utf-8'
reload = False

# load image data into MongoDB
# almost 5.00 GB data
def loadDataIntoMongoDB():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["passthemon"]
    mycol = mydb["Images"]
    start_id = 1
    if reload == True: mycol.delete_many({}) # 删除数据库所有原有文件
    else: start_id = mycol.count() + 1
    mydict = []
    
    logging.info('--------Start load image data into MongoDB--------')
    with codecs.open(filename = filename, mode = 'r', encoding = file_encoding) as file:
        items = tuple(csv.reader(file))
        length = len(items)
        logging.info(''.join(['You will start at index ', str(start_id)]))
        for i in range(start_id, length):
            goods_id = items[i][0]
            photo_url = items[i][-1]
            photo_base64 = base64.b64encode(requests.get(photo_url).content)
            # 单条插入
            myele = {"displayOrder": 1, "goods_id": int(goods_id), "_class": "com.backend.passthemon.entity.Images", "image": str(photo_base64)[2:]}
            mycol.insert_one(myele)
            # 多条插入
            # mydict.append({"displayOrder": "1", "goods_id": goods_id, "image": str(photo_base64)[2:]})
        # x = mycol.insert_many(mydict)
        # print(x)
        
logging.basicConfig(format = '%(asctime)s - %(pathname)s[line:%(lineno)d]\n%(levelname)s: %(message)s',
                    level = logging.DEBUG)
loadDataIntoMongoDB()