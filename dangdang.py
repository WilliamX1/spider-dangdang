import base64, requests, scrapy
from posixpath import join
from scrapy.crawler import CrawlerProcess
import pymysql, csv, codecs
import time, random
import os

export_filename = 'dangdang.csv'
file_encoding = 'utf-8'

# run command: python xxx.py
class DangDang(scrapy.Spider):
    name = 'clothes' 
    base_url = 'http://category.dangdang.com'
    start_urls = [  
                    # 创意文具
                    'http://category.dangdang.com/cid10009531.html', # 学生文具
                    'http://category.dangdang.com/cid10009437.html', # 笔类
                    'http://category.dangdang.com/cid10009511.html', # 纸张本册
                    'http://category.dangdang.com/cid10009454.html', # 画具/画材/书法用品
                    'http://category.dangdang.com/cid10009480.html', # 文房用品
                    'http://category.dangdang.com/cid10009493.html', # 办公用品
                    # 服饰、内衣 
                    'http://category.dangdang.com/cid4003844.html',  # 女装
                    'http://category.dangdang.com/cid10010336.html', # 男装
                    'http://category.dangdang.com/cid10010337.html', # 内衣
                    'http://category.dangdang.com/cid10010338.html', # 服饰配件
                    'http://category.dangdang.com/cid4004344.html',  # 童装童鞋
                    # 鞋靴、箱包
                    'http://category.dangdang.com/cid4001867.html',  # 女鞋
                    'http://category.dangdang.com/cid4001896.html',  # 男鞋
                    'http://category.dangdang.com/cid4009376.html',  # 童鞋
                    'http://category.dangdang.com/cid4001832.html',  # 女包
                    'http://category.dangdang.com/cid4001831.html',  # 男包
                    'http://category.dangdang.com/cid4008135.html',  # 功能包
                    'http://category.dangdang.com/cid4006430.html',  # 皮带/配件
                    # 运动户外
                    'http://category.dangdang.com/cid4002384.html', # 运动鞋
                    'http://category.dangdang.com/cid4009573.html', # 户外服饰
                    'http://category.dangdang.com/cid4009596.html', # 户外装备
                    'http://category.dangdang.com/cid4002901.html', # 健身器械
                    'http://category.dangdang.com/cid4002533.html', # 球迷用品
                    'http://category.dangdang.com/cid4010880.html', # 瑜伽用品
                    # 孕、婴、童
                    'http://category.dangdang.com/cid4009359.html', # 童装
                    'http://category.dangdang.com/cid4009376.html', # 童鞋
                    'http://category.dangdang.com/cid4004873.html', # 婴儿服饰
                    'http://category.dangdang.com/cid4001976.html', # 奶粉
                    'http://category.dangdang.com/cid4002048.html', # 辅食
                    'http://category.dangdang.com/cid4002055.html', # 尿裤
                    'http://category.dangdang.com/cid4001963.html', # 喂养用品
                    'http://category.dangdang.com/cid4001996.html', # 清洁、洗护、护肤
                    'http://category.dangdang.com/cid4002032.html', # 寝居用品
                    'http://category.dangdang.com/cid4002061.html', # 玩具童车
                    'http://category.dangdang.com/cid4004866.html', # 孕妈专区
                    # 家居、家纺、汽车
                    'http://category.dangdang.com/cid4010918.html', # 家纺
                    'http://category.dangdang.com/cid4009448.html', # 史上家饰
                    'http://category.dangdang.com/cid4010917.html', # 水具
                    'http://category.dangdang.com/cid4010916.html', # 厨具
                    'http://category.dangdang.com/cid4001637.html', # 整理收纳
                    'http://category.dangdang.com/cid4005368.html', # 清洁护理
                    'http://category.dangdang.com/cid4001637.html', # 整理收纳
                    'http://category.dangdang.com/cid4005290.html', # 健身瑜伽
                    'http://category.dangdang.com/cid4001188.html', # 宠物用品
                    'http://category.dangdang.com/cid4002429.html', # 汽车用品
                    # 家具、家装、康体
                    'http://category.dangdang.com/cid4010000.html', # 客厅家具
                    'http://category.dangdang.com/cid4010000.html', # 卧室家具
                    'http://category.dangdang.com/cid4010003.html', # 书房家具
                    'http://category.dangdang.com/cid4010004.html', # 储物家具
                    'http://category.dangdang.com/cid4009443.html', # 厨房卫浴
                    'http://category.dangdang.com/cid4009499.html', # 灯具照明
                    'http://category.dangdang.com/cid4010894.html', # 家装建材
                    'http://category.dangdang.com/cid4010858.html', # 按摩设备
                    # 美妆、个人护理、清洁
                    'http://category.dangdang.com/cid4002644.html', # 美容护肤
                    'http://category.dangdang.com/cid4002093.html', # 男士专区
                    'http://category.dangdang.com/cid4002075.html', # 口腔护理
                    'http://category.dangdang.com/cid4002077.html', # 缤纷彩妆
                    'http://category.dangdang.com/cid4002673.html', # 沐浴用品
                    'http://category.dangdang.com/cid4002636.html', # 卫生护理
                    'http://category.dangdang.com/cid4002142.html', # 香水spa
                    'http://category.dangdang.com/cid4003632.html', # 头发护理
                    'http://category.dangdang.com/cid4003638.html', # 日用清洁
                    'http://category.dangdang.com/cid4005291.html', # 两性健康
                    # 食品、茶酒
                    'http://category.dangdang.com/cid4005627.html', # 休闲食品
                    'http://category.dangdang.com/cid4003599.html', # 进口食品
                    'http://category.dangdang.com/cid4005629.html', # 粮油调味
                    'http://category.dangdang.com/cid4010964.html', # 冲调饮品
                    'http://category.dangdang.com/cid4008009.html', # 果蔬肉蛋
                    'http://category.dangdang.com/cid4010747.html', # 营养成分
                    'http://category.dangdang.com/cid4010840.html', # 滋养补品
                    # 珠宝首饰
                    'http://category.dangdang.com/cid4009606.html', # 黄/铂/K金
                    'http://category.dangdang.com/cid4009622.html', # 流行饰品
                    'http://category.dangdang.com/cid10010138.html', # 文化创意用品
                    # 手机、数码、电脑办公
                    'http://category.dangdang.com/cid4004279.html', # 手机
                    'http://category.dangdang.com/cid4001049.html', # 手机配件
                    'http://category.dangdang.com/cid4001136.html', # 视听影音
                    'http://category.dangdang.com/cid4001123.html', # 摄影摄像
                    'http://category.dangdang.com/cid4002602.html', # 数码配件
                    'http://category.dangdang.com/cid4006504.html', # 电子教育
                    'http://category.dangdang.com/cid4010235.html', # 智能设备
                    'http://category.dangdang.com/cid4002590.html', # 电脑整机
                    'http://category.dangdang.com/cid4002232.html', # 外设产品
                    'http://category.dangdang.com/cid4001077.html', # 电脑配件
                    'http://category.dangdang.com/cid4002231.html', # 网络产品
                    'http://category.dangdang.com/cid4002616.html', # 办公设备
                    'http://category.dangdang.com/cid4010714.html', # 文具耗材
                    # 家用电器
                    'http://category.dangdang.com/cid4002520.html', # 厨房电器
                    'http://category.dangdang.com/cid4002522.html', # 生活电器
                    'http://category.dangdang.com/cid4007241.html', # 大家电
                    'http://category.dangdang.com/cid4010620.html', # 液晶电视
                    'http://category.dangdang.com/cid4009636.html', # 空调
                    'http://category.dangdang.com/cid4009637.html', # 冰箱
                    'http://category.dangdang.com/cid4009638.html', # 洗衣机
                    'http://category.dangdang.com/cid4002521.html', # 个人护理
                  ]

    custom_settings = {
        'CONCURRENT_REQUESTS': 12500,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 50,
        'CONCURRENT_REQUESTS_PER_IP': 50,
        'RANDOMIZE_DOWNLOAD_DELAY': 0,
        'DOWNLOAD_DELAY': 0,
        'LOG_LEVEL' : 'INFO'
    }
    idx = 0
    def parse(self, response):
        for item in response.xpath('//*[@id="component_47"]/li'):
            # 处理图片链接
            photo_url = item.xpath('a/img/@data-original').extract_first()
            if photo_url is None: photo_url = item.xpath('a/img/@src').extract_first()
            photo_url = ''.join(['http:', photo_url])
            # 处理图片base64
            # photo_base64 = base64.b64encode(requests.get(photo_url).content)
            # 价格
            price = item.xpath('p[@class="price"]/span/text()').extract_first()
            price = price[1:]
            # 名字
            name = item.xpath('p[@class="name"]/a/text()').extract_first()
            # 说明
            description = item.xpath('p[@class="search_hot_word"]/text()').extract_first()
            # 评论数
            comment_number = item.xpath('p[@class="star"]/a/text()').extract_first()
            # 卖家
            shop = item.xpath('p[@class="link"]/a/text()').extract_first()
            # id
            DangDang.idx = DangDang.idx + 1
            yield {
                # 'photo_base64': photo_base64,
                'goods_id': DangDang.idx,
                'description': description,
                'inventory': random.randint(1, 5), # 随机生成1~5之间的数
                'name': name,
                'price': price,
                'state': 1,
                'upload_time': generateRandomTimestamp(),
                'shop': shop,
                'photo_url': photo_url,
            }

        next_url = response.css('li.next a::attr("href")').extract_first()
        if next_url is not None:
            next_url = ''.join([DangDang.base_url, next_url])
            yield scrapy.Request(next_url, callback = self.parse, dont_filter = False)

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

# remove original file
if (os.path.exists(export_filename)):
    os.remove(export_filename)
else: print(''.join([export_filename, ' not exist']))

# start spider
process = CrawlerProcess(settings = {
    'FEED_FORMAT': 'csv',
    'FEED_URI': export_filename,
    'FEED_EXPORT_ENCODING': file_encoding,
})
process.crawl(DangDang)
process.start()