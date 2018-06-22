###################################################################
# File Name: ganjijob.py
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

class GanJiSpider(RedisSpider):
    name = 'ganji_test'
    allowed_domains = []
    redis_key = 'ganji_urls'

    DEFAULT_REQUEST_HEADERS = {
      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
    }
    
    custom_settings = {
        #'DOWNLOADER_MIDDLEWARES' : {
        #            'jobwebpro.mymiddlewares.RandomProxy' : 1,
        #            'jobwebpro.mymiddlewares.RandomUA':2
        #},

        'DOWNLOAD_DELAY': 2,
    
        'CONCURRENT_REQUESTS' : 50,
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
            'db': 0
            },
        }
    
    def parse(self, response):
        privance = response.xpath('//div[@class="all-city"]//a/@href').extract()
        print(privance)
        for city in privance:
            fullUrl = request.urljoin(city,'zhaopin')
            yield scrapy.Request(fullUrl, callback=self.parseIndustry, meta={'baseurl':city})
            
    def parseIndustry(self, response):
        # print(response.status)
        typeIndustry = response.xpath('//div[@class="f-con-right fl js-content-container"]//dd/a/@href').extract()[:-3]
        baseurl = response.meta['baseurl']
        for type in typeIndustry:
            fullUrl = request.urljoin(baseurl, type)
            yield scrapy.Request(fullUrl, callback=self.parseJobList)
            
    def parseJobList(self, response):
        jobList = response.xpath('//div[@class="new-dl-wrapper"]/div/dl')

        for job in jobList:
            jobLink = job.xpath('./dt/a[@class="list_title gj_tongji"]/@href').extract()[0]
            item = RedisItem()
            item['jobname'] = job.xpath('./dt/a[@class="list_title gj_tongji"]/text()').extract()[0]
            salary = job.xpath('./dd[@class="company"]/div[@class="new-dl-salary"]/text()').extract()[0]
            if '-' in salary:
                item['salarytop'] = int(salary.split('元')[0].split('-')[-1])
                item['salarybottom'] = int(salary.split('元')[0].split('-')[0])
            elif '以上' in salary:
                item['salarytop'] = 20001
                item['salarybottom'] = 20000
            elif '以下' in salary:
                item[ 'salarytop' ] = 1000
                item[ 'salarybottom' ] = 0
            elif '面议' in salary:
                item[ 'salarytop' ] = 0
                item[ 'salarybottom' ] = 0
            item['workaddr'] = job.xpath('./dd[@class="pay"]/text()').extract()[0]
            item['company'] = job.xpath('./dt/div[@class="new-dl-company"]/a/text()').extract()[0]
            item['spidername'] = self.name

            
            yield scrapy.Request(jobLink, callback=self.parseJobInfo, meta={'item':item})
        
    def parseJobInfo(self, response):
        item = response.meta['item']    
        
        companyaddr = response.xpath('//div[@class="location-line clearfix"]/p/text()').extract()
        item['companyaddr'] = companyaddr[0].replace('-',' ').replace(' ','').lstrip('\n')
        
        jobdesc = response.xpath('//div[@class="module-description"]//div[@data-role="description"]/text()').extract()
        full_desc = ''
        for i in jobdesc:
            i = i.replace(' ','').strip('\n').strip('\r').strip('\r\n') + '-' 
            full_desc += i
        item['jobdesc'] = full_desc
        jobInfo = response.xpath('//div[@class="module-description"]/div[@class="description-label"]/span/text()').extract()
        
        # 学历处理
        if '不限' in jobInfo[1]: 
            item['eduction'] = '不限'
        elif '要求' in jobInfo[1]:
            item['eduction'] = jobInfo[1].rstrip('学历').lstrip('要求')

        # 经验处理
        if '不限' in jobInfo[1]: 
           item['workyear'] = '经验不限'
        elif '要求' in jobInfo[1]:
           item['workyear'] = jobInfo[-2].rstrip('年工作经验').lstrip('要求')
        

        jobPub = response.xpath('//div[@class="left-box"]//p[1]/span[1]/text()').extract()
        jobPub = jobPub[0].split('：')[-1]    
        if jobPub.count('-') == 2:
            item['jobpub'] = jobPub
        elif jobPub.count('-') == 1:
            item['jobpub'] = '18-' + jobPub
        else:
            item['jobpub'] = datetime.datetime.now().strftime('%y-%m-%d')
        item['url'] = response.url
        
        
        yield item 
        
