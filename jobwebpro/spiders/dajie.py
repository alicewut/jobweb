###################################################################
# File Name: dajie.py
# Author: alice
# mail: alicewut@163.com
# Created Time: Tue 29 May 2018 10:17:33 AM CST
#=============================================================
#!/usr/bin/python
# -*- coding: utf-8 -*-
import scrapy
from scrapy_redis.spiders import RedisSpider
from urllib import request
from jobwebpro.items import RedisItem
import datetime
import json, jsonpath
import datetime

class DaJieSpider(RedisSpider):
    name = 'dajie'
    allowed_domains = []
    # start_urls = ['http://www.dajiao.com']
    redis_key = 'dajie_urls'

    custom_settings = {
       'DOWNLOADER_MIDDLEWARES' : {
        #            'jobwebpro.mymiddlewares.RandomProxy' : 1,
                    'jobwebpro.mymiddlewares.RandomUA':2
       },

        'DOWNLOAD_DELAY': 0,
    
        'CONCURRENT_REQUESTS': 30,
        # 下载超时 5- 10 秒
        'DOWNLOAD_TIMEOUT': 10,
        # 下载重试次数 2 -3 次
        'RETRY_TIMES': 2,


        # -----------------------scrapy redis 配置---------------------------------------

        #url指纹过滤器
        'DUPEFILTER_CLASS': "scrapy_redis.dupefilter.RFPDupeFilter",
    
        # 调度器
        'SCHEDULER': "scrapy_redis.scheduler.Scheduler",
        # 设置爬虫是否可以中断
        'SCHEDULER_PERSIST': True,
    
        # 设置请求队列类型
        # SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderPriorityQueue" # 按优先级入队列
        # 'SCHEDULER_QUEUE_CLASS': "scrapy_redis.queue.SpiderQueue",  # 按照队列模式
        'SCHEDULER_QUEUE_CLASS' : "scrapy_redis.queue.SpiderStack", # 按照栈进行请求的调度
    
        # 配置redis管道文件，权重数字相对最大
        'ITEM_PIPELINES': {
            'scrapy_redis.pipelines.RedisPipeline': 999,  # redis管道文件，自动把数据加载到redis
        },
    
        # redis 连接配置
        'REDIS_HOST': '127.0.0.1',
        'REDIS_PORT': 6379,
        'REDIS_PARAMS': {
            'password': '123456',
            'db': 1
            },
        }
    
    def parse(self, response):
        baseUrl = 'https://www.dajie.com/ajax/index/jobs?ajax=1&type=2&page={}&pageSize=500&_CSRFToken='
        for i in range(1,342):
            fullUrl = baseUrl.format(str(i))
            yield scrapy.Request(fullUrl, callback=self.parseList) 

    # 解析json文件        
    def parseList(self, response):
        jsonfile = response.text
        s = json.loads(jsonfile)
        res = jsonpath.jsonpath(s, '$.data.content.data')
        for i in res[0]:
            print(i)
            print("===============================")
            item = RedisItem()
            item['jobname'] = i['jobName']
            item['workyear'] = i['experience']
            item['url'] = i['clickUrl']
            item['eduction'] = i['degree']
            item['company'] = i['corpName']
            item['workaddr'] = i['city']
            item['spidername'] = self.name
            salary = i['salary']

            yield scrapy.Request(i['clickUrl'], callback=self.parseInfo, meta={'item': item, 'salary':salary})

    
    # 解析详情页
    def parseInfo(self, response):
        item = response.meta['item']
        salary = response.meta['salary']
        
        # 处理薪水
        if '日' in salary:
            money = salary.split('/')[0]
            item['salarytop'] = int(money) * 21 + 1
            item['salarybottom'] = int(money) * 21
        elif '时' in salary:
            money = salary.split('/')[0]
            item['salarytop'] = int(money) * 8 * 21 + 1
            item['salarybottom'] = int(money) * 8 * 21
        elif '-' in salary:
            moneyrange = salary.split('/')[0].split('-')
            item['salarytop'] = int(moneyrange[-1].rstrip('K')) * 1000
            item['salarybottom'] = int(moneyrange[0].rstrip('K')) * 1000

        elif '+' in salary:
            item['salarybottom'] = int(salary.split('+')[0].rstrip('K')) * 1000
            item['salarytop'] = item['salarybottom'] + 1
        else:
            item['salarytop'] = 0
            item['salarybottom'] = 0

        desc = response.xpath('//div[@id="jp_maskit"]/pre/text()').extract()[0]
        item['jobdesc'] = desc
        
        item['jobpub'] = datetime.datetime.now().strftime('%y-%m-%d')
        item['companyaddr'] = response.xpath('//div[@id="jp-app-wrap"]/div[@class="p-wrap-box"]/div[@class="p-side-left"]/div[@class="ads-msg"]/span').extract()[0]

        yield item
        

