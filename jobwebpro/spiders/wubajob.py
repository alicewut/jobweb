###################################################################
# File Name: wubajob.py
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

class WuBaSpider(RedisSpider):
    name = 'wuba'
    allowed_domains = []
    redis_key = 'wuba_urls'

    custom_settings = {
       'DOWNLOADER_MIDDLEWARES' : {
        #            'jobwebpro.mymiddlewares.RandomProxy' : 1,
                    'jobwebpro.mymiddlewares.RandomUA':2
       },

        'DOWNLOAD_DELAY': 0.5,
    
        'CONCURRENT_REQUESTS' : 30,
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
            'db': 2
            },
        }
    
    def parse(self, response):
        privance = response.xpath('//div[@class="topcity"]/dl[@id="clist"]//dd//a/@href').extract()
        for city in privance:
            if 'http://g.58.com/' in city:
                continue
            for i in range(1, 71):
                addVal = "job/pn{}".format(str(i))
                fullUrl = request.urljoin(city,addVal)
                yield scrapy.Request(fullUrl, callback=self.parseList, meta={'baseurl':city})
            
    def parseList(self, response):
        liList = response.xpath('//ul[@id="list_con"]/li')
        for job in liList:
            jobUrl = job.xpath('./div/div[@class="job_name clearfix"]/a/@href').extract()[0]

            eduction = job.xpath('./div[@class="item_con job_comp"]/p[@class="job_require"]/span[2]/text()').extract()[0]

            workyear = job.xpath('./div[@class="item_con job_comp"]/p[@class="job_require"]/span[3]/text()').extract()[0]
            print(jobUrl, '=====================',eduction,'===================', workyear)
            yield scrapy.Request(jobUrl, callback=self.parseJobList, meta={'eduction':eduction, 'workyear':workyear})


            
    
            
    def parseJobList(self, response):
        item = RedisItem()
        eduction = response.meta['eduction']
        
        workyear = response.meta['workyear']
        jobname = response.xpath('//div[@class="pos_base_info"]/span[@class="pos_title"]/text()').extract()[0]
        salary= response.xpath('//div[@class="pos_base_info"]/span[2]/text()').extract()[0]
        if '面' in salary:
            salarytop = 0
            salarybottom = 0
        else:
            salarytop = salary.split('-')[-1]
            salarybottom = salary.split('-')[0]
        company = response.xpath('//div[@class="baseInfo_link"]/a/text()').extract()[0]
        jobdesc = response.xpath('//div[@class="des"]/text()').extract()
        fulldesc = ''
        for i in jobdesc:
            i = i.strip('\r\n').strip('\r').strip('\n').replace('\t', '').replace('\r\n','').replace(' ','') + '-'
            fulldesc += i
        resUrl = response.url
        workaddr = response.xpath('//div[@class="pos-area"]/span[1]/span/text()').extract()
        workaddr = ' '.join(workaddr).replace('-','').replace(' ','').replace('\n', '').replace('\r', '').replace('\r\n', '')
        companyaddr = response.xpath('//div[@class="pos-area"]/span[2]/text()').extract()
        if not companyaddr:
            companyaddr = workaddr
        else:
            companyaddr = companyaddr[0]
        jobPub = response.xpath('//div[@class="pos_base_statistics"]/span[@class="pos_base_num pos_base_update"]/span/text()').extract()[0]
        if '今天' in jobPub:
            jobPub = datetime.datetime.now().strftime('%y-%m-%d')
        elif '天前' in jobPub:
            jobPub = datetime.datetime.now().strftime('%y-%m-%d')

        spidername = self.name

        item['jobname'] = jobname
        item['salarytop'] = salarytop
        item['salarybottom'] = salarybottom
        item['companyaddr'] = companyaddr
        item['workyear'] = workyear
        item['workaddr'] = workaddr
        item['eduction'] = eduction
        item['jobpub'] = jobPub
        item['spidername'] = self.name
        item['jobdesc'] = fulldesc 
        item['company'] = company
        item['url'] = response.url

        yield item


     #   item['jobname'] = job.xpath('./dt/a[@class="list_title gj_tongji"]/text()').extract()[0]
    #        salary = job.xpath('./dd[@class="company"]/div[@class="new-dl-salary"]/text()').extract()[0]
    #        if '-' in salary:
    #            item['salarytop'] = int(salary.split('元')[0].split('-')[-1])
    #            item['salarybottom'] = int(salary.split('元')[0].split('-')[0])
    #        elif '以上' in salary:
    #            item['salarytop'] = 20001
    #            item['salarybottom'] = 20000
    #        elif '以下' in salary:
    #            item[ 'salarytop' ] = 1000
    #            item[ 'salarybottom' ] = 0
    #        elif '面议' in salary:
    #            item[ 'salarytop' ] = 0
    #            item[ 'salarybottom' ] = 0
    #        item['workaddr'] = job.xpath('./dd[@class="pay"]/text()').extract()[0]
    #        item['company'] = job.xpath('./dt/div[@class="new-dl-company"]/a/text()').extract()[0]
    #        item['spidername'] = self.name

    #        
    #        yield scrapy.Request(jobLink, callback=self.parseJobInfo, meta={'item':item})
    #    
    #def parseJobInfo(self, response):
    #    item = response.meta['item']    
    #    companyaddr = response.xpath('//div[@class="location-line clearfix"]/p/text()').extract()
    #    if companyaddr:
    #        item['companyaddr'] = companyaddr[0].replace('-',' ').replace(' ','').lstrip('\n')
    #    else:
    #        item['companyaddr'] = item['workaddr']
    #    jobdesc = response.xpath('//div[@class="module-description"]//div[@data-role="description"]/text()').extract()
    #    if jobdesc:
    #        item['jobdesc'] = response.xpath('//div[@class="module-description"]//div[@data-role="description"]/text()').extract()[0].strip('\n')
    #    else:
    #        item['jobdesc'] = '暂无数据'
    #    jobInfo = response.xpath('//div[@class="module-description"]/div[@class="description-label"]/span/text()').extract()
    #    print(jobInfo)
    #    if jobInfo:
    #        # 学历处理
    #        if '不限' in jobInfo[1]: 
    #            item['eduction'] = '不限'
    #        elif '要求' in jobInfo[1]:
    #            item['eduction'] = jobInfo[1].rstrip('学历').lstrip('要求')

    #        # 经验处理
    #        if '不限' in jobInfo[1]: 
    #            item['workyear'] = '经验不限'
    #        elif '要求' in jobInfo[1]:
    #            item['workyear'] = jobInfo[-2].rstrip('年工作经验').lstrip('要求')
    #    
    #    else:
    #        item['workyear'] = '暂无信息'
    #        item['eduction'] = '暂无信息'

    #    jobPub = response.xpath('//div[@class="left-box"]//p[1]/span[1]/text()').extract()
    #    if jobPub:
    #        jobPub = jobPub[0].split('：')[-1]    
    #        if jobPub.count('-') == 2:
    #            item['jobpub'] = jobPub
    #        elif jobPub.count('-') == 1:
    #            item['jobpub'] = '18-' + jobPub
    #        else:
    #            item['jobpub'] = datetime.datetime.now().strftime('%y-%m-%d')
    #    else:
    #        item['jobpub'] = '暂无信息'
    #    
    #    
    #    yield item 
    #    
