###################################################################
# File Name: mymiddleware.py
# Author: alice
# mail: alicewut@163.com
# Created Time: Tue 29 May 2018 08:45:13 PM CST
#=============================================================
#!/usr/bin/python
from fake_useragent import UserAgent
from .public import publicfunc


class RandomUA(object):
    def __init__(self):
        self.ua = UserAgent()
    def process_request(self, request, spider):
        request.headers['User-Agent'] = self.ua.random

class RandomProxy(object):
    def process_request(self, request, spider):
        host,port = publicfunc.sqlRandom()
        print(host, port)
        p = 'http://%s:%s' % (host,port)
        request.meta['proxy'] = p
