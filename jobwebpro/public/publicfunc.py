# -*- coding:utf-8 -*-
# Author : WuTao
# Date : 2018-05-29 2:12 PM 
# ProjectName : jobwebpro 
# FileName : publicfunc

import pymysql

def sqlRandom():
    db = pymysql.connect('10.15.112.21', 'alice', '753159wt', 'jobweb', charset='utf8')
    cursor = db.cursor()
    sql = 'select host,port from proxypool order by rand() limit 1'
    cursor.execute(sql)
    res = cursor.fetchone()
    return res[0],res[1]

if __name__ == '__main__':
    sqlRandom()

