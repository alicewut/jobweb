###################################################################
# File Name: ganjidata.py
# Author: alice
# mail: alicewut@163.com
# Created Time: Wed 30 May 2018 02:18:50 PM CST
#=============================================================
#!/usr/bin/python
# -*- coding: utf-8 -*-

# 赶集网数据落地

import json
import redis  # pip install redis
import pymysql

def main():
    # 指定redis数据库信息
    rediscli = redis.StrictRedis(host='10.15.112.21',password = 'a11112222', port = 6379, db = 0)
    # 指定mysql数据库
    mysqlcli = pymysql.connect(host='10.15.112.21', user='alice', passwd='753159wt', db='jobweb', charset='utf8')

    # 无限循环
    while True:
        source, data = rediscli.blpop(["ganji:items"]) # 从redis里提取数据
        item = json.loads(data.decode('utf-8')) # 把 json转字典
        print(item)
        try:
            # 使用cursor()方法获取操作游标
            cur = mysqlcli.cursor()
            # 使用execute方法执行SQL INSERT语句
            sql = 'insert into jobinfo(jobname,salarytop,salarybottom,eduction,workyear,workaddr,company,companyaddr,jobdesc,jobpub,spidername) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            cur.execute(sql, (item["jobname"], item["salarytop"], item["salarybottom"], item["eduction"], item["workyear"], item["workaddr"], item["company"],item["companyaddr"], item["jobdesc"], item["jobpub"], item["spidername"]))
                                                                                                         # 提交sql事务
            mysqlcli.commit()
            #关闭本次操作
            cur.close()
            print ("插入 %s" % item['jobname'])
        except pymysql.Error as e:
            mysqlcli.rollback()
            print ("插入错误" ,str(e))


if __name__ == '__main__':
    main()
