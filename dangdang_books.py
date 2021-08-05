import base64, requests, scrapy
from posixpath import join
from scrapy.crawler import CrawlerProcess
import pymysql, csv, codecs
import time, random
import os

export_filename = 'dangdang_books.csv'
file_encoding = 'utf-8'

class DangDangBooks(scrapy.Spider):
    name = 'books'
    base_url = 'http://bang.dangdang.com/books/bestsellers/01.00.00.00.00.00-recent30-0-0-1-'
    idx, max = 1, 50
    start_urls = [
        'http://bang.dangdang.com/books/bestsellers/01.00.00.00.00.00-recent30-0-0-1-1',
    ]
    custom_settings = {
        'CONCURRENT_REQUESTS': 125,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 50,
        'CONCURRENT_REQUESTS_PER_IP': 50,
        'RANDOMIZE_DOWNLOAD_DELAY': 0,
        'DOWNLOAD_DELAY': 0
    }
    
    def parse(self, response):
        for item in response.css('ul.bang_list.clearfix.bang_list_mode li'):
            id = item.xpath('div[1]/text()').extract_first()
            id = id[:-1]
            photo_url = item.xpath('div[@class="pic"]/a/img/@src').extract_first()
            photo_base64 = base64.b64encode(requests.get(photo_url).content)
            
            name = item.xpath('div[@class="name"]/a/text()').extract_first()
            comment_number = item.xpath('div[@class="star"]/a/text()').extract_first()
            author = item.xpath('div[5]/a/text()').extract_first()
            publish_time = item.xpath('div[6]/span/text()').extract_first()
            publisher = item.xpath('div[6]/a/text()').extract_first()
            
            price = item.xpath('div[@class="price"]/p/span[@class="price_n"]/text()').extract_first()
            price = price[1:]
            
            yield {
                'id': id,
                'photo_url': photo_url,
                'name': name,
                'comment_number': comment_number,
                'author': author,
                'publish_time': publish_time,
                'publisher': publisher,
                'price': price,
                # 'photo_base64': photo_base64
            }
        DangDangBooks.idx = DangDangBooks.idx + 1
        next_url = ''.join([DangDangBooks.base_url, str(DangDangBooks.idx)])
        if DangDangBooks.idx <= DangDangBooks.max:
            yield scrapy.Request(next_url, callback = self.parse, dont_filter = False)

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
process.crawl(DangDangBooks)
process.start()