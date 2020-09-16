import os
import sys
import time
import datetime
import json
import pymysql
import configparser

import settings as Set

class Base:
    def __init__(self, date):
        self.date = date
        self.lastDay = date - datetime.timedelta(days=1)
        self.lastMonth = date - datetime.timedelta(days=int(date.strftime('%d')))

        self.strDay = date.strftime('%Y-%m-%d')
        self.strMonth = date.strftime('%Y-%m')

        self.strLastDay = self.lastDay.strftime('%Y-%m-%d')
        self.strLastMonth = self.lastMonth.strftime('%Y-%m')

        self.strTableMonth = date.strftime('_%Y_%m')

        self.coon, self.cur = self.connectSQL(self.readConfig())
        self.file = self.openFile()

    def readConfig(self):
        db = {}

        config = configparser.ConfigParser()
        # path = os.path.dirname(os.path.realpath(sys.executable))
        path = os.path.dirname(__file__)
        config.read('%s\\conf.ini' % path)

        for item in config.items('db'):
            db[item[0]] = item[1]

            if item[0] == 'port':
                db[item[0]] = int(item[1])

        return db

    # 连接数据库
    def connectSQL(self, confDB):
        coon = pymysql.connect(host=confDB['host'],
                               port=confDB['port'],
                               user=confDB['user'],
                               passwd=confDB['passwd'],
                               db=confDB['db'],
                               charset=confDB['charset'])

        return coon, coon.cursor()

    # 打开日志文件
    def openFile(self):
        path = os.path.dirname(os.path.realpath(sys.executable))
        if not os.path.exists('%s\\log' % path):
            os.mkdir('%s\\log' % path)

        filePath = '%s\\log\\%s.txt' % (path, self.strDay)

        try:
            File = open(filePath, 'a', encoding='utf-8')
        except IOError as e:
            print(e)
        else:
            return File

    # 写日志文件
    def writeFile(self, info, level=''):
        try:
            localTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            self.file.write('%swriteTime:%s; %s\n' % (level, localTime, info))
        except Exception as e:
            self.printResult(e, "error:  ")

    # 输出日志文件,基本信息
    def printInfo(self, date, sign, dataNum, other=''):
        strPrint = "date:%s; sign:%s; dataNum:%s; %s" % (date, sign, dataNum,
                                                         other)
        self.writeFile(strPrint)
        print(strPrint)

    # 输出日志文件,结果信息
    def printResult(self, info, level):
        self.writeFile(info, level)
        print(info)

    def __del__(self):
        self.cur.close()
        self.coon.close()
        self.file.close()
