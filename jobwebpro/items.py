# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JobwebproItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class RedisItem(scrapy.Item):
    jobname = scrapy.Field()
    salarytop = scrapy.Field()
    salarybottom = scrapy.Field()
    eduction = scrapy.Field()
    workyear = scrapy.Field()
    workaddr = scrapy.Field()
    company = scrapy.Field()
    companyaddr = scrapy.Field()
    jobdesc = scrapy.Field()
    jobpub = scrapy.Field()
    spidername = scrapy.Field()
    url = scrapy.Field()

    
