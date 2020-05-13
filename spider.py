import os
import requests
import time
import datetime
import json
import pymysql

import settings as Set


class Spider:
    def __init__(self, date):
        self.date = date
        self.strDate = date.strftime('%Y-%m-%d')
        self.strLastDate = (date -
                            datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        self.coon, self.cur = self.connectSQL()
        self.file = self.openFile()
        self.session = self.login()

    # 连接数据库
    def connectSQL(self):
        coon = pymysql.connect(host=Set.SQL['host'],
                               port=Set.SQL['port'],
                               user=Set.SQL['user'],
                               passwd=Set.SQL['passwd'],
                               db=Set.SQL['db'],
                               charset=Set.SQL['charset'])

        return coon, coon.cursor()

    # 打开日志文件
    def openFile(self):
        if not os.path.exists('log'):
            os.mkdir('log')

        filePath = 'log\\%s.txt' % (self.strDate)

        try:
            File = open(filePath, 'a', encoding='utf-8')
        except IOError as e:
            print(e)
        else:
            return File

    # 数据插入对应表
    def insertSQL(self, tableName, colunm, datas):
        if len(datas) == 0:
            self.printResult(
                "table(%s):Data write error; datas is empty." % tableName,
                "error:  ")
            return

        if not self.isEnoughData(tableName, len(datas)):
            self.printResult(
                "table(%s):Data write error; datas is not enough." % tableName,
                "error:  ")
            return

        valuesNum = []
        for i in range(len(colunm)):
            valuesNum.append('%s')

        valuesNum = tuple(valuesNum)

        sql = "INSERT INTO %s%s VALUES%s;" % (tableName, colunm, valuesNum)
        sql = sql.replace("\'", "")

        try:
            self.cur.executemany(sql, datas)
            self.coon.commit()
            self.printResult("table(%s):Data write successfully." % tableName,
                             "succ:  ")
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()

    # 通过与前一日数据量比对，判断数据爬取是否成功
    def isEnoughData(self, tableName, dataNum):
        if tableName == 'line_abnormal_info' or tableName == 'tg_abnormal_info':
            return True

        sql = " SELECT statDate \
                FROM %s \
                WHERE statDate='%s';" % (tableName, self.strLastDate)

        try:
            lastNum = self.cur.execute(sql)
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return False

        if lastNum == 0:
            return True

        cRate = round(abs(dataNum - lastNum) / lastNum, 3)
        self.printResult(
            "table(%s):The number of data is %s;" % (tableName, cRate),
            "tips:  ")

        if cRate <= 0.050:
            return True
        else:
            return False

    # 查询线路表中当日数据的线路ID和线路Type
    def selectLineSQL(self):
        sql = " SELECT lineId,lineNo,lineName,lineType \
                FROM line_power_info \
                WHERE statDATE='%s';" % self.strDate

        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return ()
        else:
            return res

    # 查询台区表中当日数据的台区ID
    def selectTGSQL(self):
        sql = " SELECT tg.tgId \
                FROM tg_power_info AS tg \
                INNER JOIN byq_tgcode AS byq \
                WHERE tg.tgNo=byq.tgNo \
                AND tg.statDATE='%s';" % self.strDate

        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return ()
        else:
            return res

    # 查询对应表中当日数据的数量
    def selectDateSQL(self, sign):
        if sign == 'IRC':
            sql = "SELECT statDate FROM %s WHERE statDate='%s';" % (
                Set.TABLE_NAME[sign], self.strLastDate)
        else:
            sql = "SELECT statDate FROM %s WHERE statDate='%s';" % (
                Set.TABLE_NAME[sign], self.strDate)

        try:
            res = self.cur.execute(sql)
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return -1
        else:
            return res

    # 判断是否已存在当天数据
    def isExist(self, sign):
        date = self.strDate
        if sign == 'IRC':
            date = self.strLastDate

        num = self.selectDateSQL(sign)
        if num < 0:
            print("\n数据类型：%s；数据日期：%s；状态：数据查询错误；" % (Set.SIGN[sign], date))
            return True
        elif num > 0:
            print("\n数据类型：%s；数据日期：%s；数据量：%s；状态：已执行；" %
                  (Set.SIGN[sign], date, num))
            return True
        else:
            print("\n数据类型：%s；状态：未执行；开始执行..." % (Set.SIGN[sign]))
            return False

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

    # 模拟登录
    def login(self):
        # 请求页面获取cookies
        response = requests.post(Set.URL['login'])
        cookies = requests.utils.dict_from_cookiejar(response.cookies)

        # 利用获取到的cookies，加上用户名密码登录，获得带tickets地址
        response = requests.post(Set.URL['login'],
                                 data=Set.LOGIN_DATA,
                                 cookies=cookies,
                                 allow_redirects=False)

        # 访问带tickets地址网址实现登录
        url = '%s' % response.headers['Location']
        session = requests.session()
        session.get(url, allow_redirects=False)

        return session

    # 向页面发送请求，并返回获得的数据
    def requestData(self, sign, tupleData, waitTime=Set.WAIT_TIME):
        if len(Set.PARAMS.get(sign, '')) == 0:
            try:
                params = {
                    'statDate': tupleData[0],
                    'orgId': tupleData[1],
                    'tgId': tupleData[2],
                    'meterId': 'null'
                }
                respon = self.session.get(Set.URL[sign], params=params)
                datas = json.loads(respon.text)
            except Exception as e:
                self.printResult(e, "error:  ")
                time.sleep(waitTime)
                return []
            else:
                time.sleep(waitTime)
                return datas
        else:
            try:
                params = {'params': Set.PARAMS[sign] % tupleData}
                respon = self.session.get(Set.URL[sign], params=params)
                datas = json.loads(respon.text)
                datas = datas['resultValue']['items']
            except Exception as e:
                self.printResult(e, "error:  ")
                time.sleep(waitTime)
                return []
            else:
                time.sleep(waitTime)
                return datas

    # 处理数据，将list中的dict转化为tuple，供插入数据使用
    def processData(self, sign, datas, tgId=''):
        values = []
        if tgId == '':
            for data in datas:
                value = []
                for colunmName in Set.TABLE[sign]:
                    value.append(data.get(colunmName, ''))

                values.append(tuple(value))
        else:
            for data in datas:
                if len(datas[data]) == 0:
                    break
                value = []
                value.append(tgId)
                value.append(self.strDate)
                if sign == 'TGEC' or sign == 'TGEV':
                    value.append(data)
                if len(datas[data]) < 24:
                    for i in range(24 - len(datas[data])):
                        value.append('0')
                value += datas[data]

                values.append(tuple(value))

        return values

    # 爬取台区异常信息
    def TG_Abnormal_Info(self):
        sign = 'TGAI'
        datas = []

        for Type in Set.ABNORMAL_TYPE:
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (self.strDate, Type, page, Set.PAGE_SIZE))
                for subData in data:
                    subData['ABNORMAL_TYPE'] = Type
                    subData['statDate'] = subData['STAT_DATE']

                datas += data

                self.printInfo(self.strDate, sign, len(datas),
                               'Type:%s; page:%s; ' % (Type, page))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 爬取线路异常信息
    def Line_Abnormal_Info(self):
        sign = 'LAI'
        datas = []

        for Type in Set.ABNORMAL_TYPE:
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (Type, self.strDate, page, Set.PAGE_SIZE))
                for subData in data:
                    subData['ABNORMAL_TYPE'] = Type
                    subData['statDate'] = subData['STAT_DATE']

                datas += data

                self.printInfo(self.strDate, sign, len(datas),
                               'Type:%s; page:%s; ' % (Type, page))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 爬取线路电量信息
    def Line_Power_Info(self):
        sign = 'LPI'
        datas = []

        for page in range(1, 1000):
            data = self.requestData(sign, (self.strDate, page, Set.PAGE_SIZE))
            datas += data

            self.printInfo(self.strDate, sign, len(datas),
                           'page:%s; ' % (page))

            if len(data) < Set.PAGE_SIZE:
                break

        return self.processData(sign, datas)

    # 爬取线路关口电量信息
    def Gateway_Power_Info(self):
        sign = 'GPI'
        datas = []

        for orgId in Set.ORG_ID:
            if orgId == Set.ORG_ID[0]:
                continue
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (orgId, self.strDate, page, Set.PAGE_SIZE))

                for subData in data:
                    subData["outIn"] = subData['inOut']
                    subData["statDate"] = subData['dataDate']

                datas += data

                self.printInfo(self.strDate, sign, len(datas),
                               'page:%s; orgId:%s; ' % (page, orgId))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 爬取台区电量信息
    def TG_Power_Info(self):
        sign = 'TGPI'
        datas = []
        lines = self.selectLineSQL()

        for line in lines:
            data = self.requestData(sign, (line[0], self.strDate))

            for subData in data:
                subData["lineId"] = line[0]

            datas += data

            self.printInfo(self.strDate, sign, len(datas),
                           'lineId:%s; ' % (line[0]))

        return self.processData(sign, datas)

    # 爬取台区和专变（高压用户）电表码值信息
    def TG_Table_Value(self):
        sign = 'TGTV'
        datas = []
        lines = self.selectLineSQL()

        for line in lines:
            data = self.requestData(sign, (line[0], self.strDate, line[3]))

            for subData in data:
                subData["PBLineId"] = line[0]
                subData["PBLineNo"] = line[1]
                subData["PBLineName"] = line[2]
                subData["PBLineType"] = line[3]

            datas += data

            self.printInfo(self.strDate, sign, len(datas),
                           'lineId:%s; lineType:%s; ' % (line[0], line[3]))

        return self.processData(sign, datas)

    # 爬取分区域完成度
    def Region_Completion(self):
        sign = 'RC'
        datas = []

        for orgId in Set.ORG_ID:
            if orgId == '1C3DAEFA42964361BCF73D24F19127F9':
                data = self.requestData(
                    'RC1', (self.strDate, self.strDate, orgId, '04'))
            else:
                data = self.requestData(
                    'RC1', (self.strDate, self.strDate, orgId, '05'))

            datas += data

            self.printInfo(self.strDate, sign, len(datas))

        data = self.requestData('RC2', (Set.ORG_ID[0]))

        for subDatas in datas:
            subDatas['statDate'] = subDatas['STAT_DATE']
            for subData in data:
                if subDatas['ORG_ID'] == subData['ORG_ID']:
                    subDatas['RATE_LOSS'] = subData['RATE_LOSS']
                    subDatas['STAND_PERCE'] = subData['STAND_PERCE']

        self.printInfo(self.strDate, sign, len(datas))

        return self.processData(sign, datas)

    # 爬取220kv线损率基值
    def Base_Rate_220(self, data, data220):
        if len(data220) != 0:
            data['baseRate'] = data220[0]['RATE_LOSS']
            data['standPerce'] = data220[0]['STAND_PERCE']
        else:
            data['baseRate'] = ''
            data['standPerce'] = ''

        return data

    # 爬取110kv线损率基值
    def Base_Rate_110(self, data, data110):
        if len(data110) != 0:
            data['baseRate'] = data110[0]['RATE_LOSS']
            data['standPerce'] = data110[0]['STAND_PERCE']
        else:
            data['baseRate'] = ''
            data['standPerce'] = ''

        return data

    # 爬取35kv线损率基值
    def Base_Rate_35(self, data, data35):
        quarter = (int(self.date.strftime('%m')) - 1) // 3 + 1

        if len(data35) == 0:
            data['baseRate'] = ''
            data['standPerce'] = ''
            return data

        for subData35 in data35:
            if subData35['QUARTER'] is None:
                if data['orgId'] == Set.ORG_ID[1] or data[
                        'orgId'] == subData35['ORG_ID']:
                    data['baseRate'] = subData35['RATE_LOSS']
                    data['standPerce'] = subData35['STAND_PERCE']
                    break
            elif int(subData35['QUARTER']) == quarter and data['orgId'] == subData35['ORG_ID']:
                data['baseRate'] = subData35['RATE_LOSS']
                data['standPerce'] = subData35['STAND_PERCE']
                break

        return data

    # 爬取10kv线损率基值
    def Base_Rate_10(self, data, data10):
        if len(data10) == 0:
            data['baseRate'] = ''
            data['standPerce'] = ''
            return data

        for subData10 in data10:
            if data['orgId'] == subData10['ORG_ID']:
                data['baseRate'] = subData10['BASE_RATE']
                if data['orgId'] == Set.ORG_ID[0]:
                    data['standPerce'] = '1.5'
                else:
                    data['standPerce'] = '2.0'

                break

        return data

    # 爬取380v线损率基值
    def Base_Rate_380(self, data, data380):
        if len(data380) == 0:
            data['baseRate'] = ''
            data['standPerce'] = ''
            return data

        for subData380 in data380:
            if data['orgId'] == subData380['ORG_ID']:
                data['baseRate'] = subData380['BASE_RATE']
                if data['orgId'] == Set.ORG_ID[0]:
                    data['standPerce'] = '1.5'
                else:
                    data['standPerce'] = '2.0'

                break

        return data

    # 爬取线损率基值，供分压完成度使用
    def Base_Rate(self, datas):
        sign = 'VC'
        strLastMonth = (self.date - datetime.timedelta(
            days=int(self.date.strftime('%d')))).strftime('%Y-%m')

        data220 = self.requestData('VC3', ('33', Set.ORG_ID[0]))
        self.printInfo(self.strDate, sign, len(datas), 'voltLevel:220kv; ')

        data110 = self.requestData('VC3', ('32', Set.ORG_ID[0]))
        self.printInfo(self.strDate, sign, len(datas), 'voltLevel:110kv; ')

        data35 = self.requestData('VC3', ('25', Set.ORG_ID[0]))
        self.printInfo(self.strDate, sign, len(datas), 'voltLevel:35kv; ')

        data10 = self.requestData('VC4', (strLastMonth))
        self.printInfo(self.strDate, sign, len(datas), 'voltLevel:10kv; ')

        data380 = self.requestData('VC5', (strLastMonth))
        self.printInfo(self.strDate, sign, len(datas), 'voltLevel:380v; ')

        for data in datas:
            if data['ve'] == '33':
                data = self.Base_Rate_220(data, data220)
            elif data['ve'] == '32':
                data = self.Base_Rate_110(data, data110)
            elif data['ve'] == '25':
                data = self.Base_Rate_35(data, data35)
            elif data['ve'] == '22':
                data = self.Base_Rate_10(data, data10)
            elif data['ve'] == '08':
                data = self.Base_Rate_380(data, data380)

        return datas

    # 爬取分压完成度
    def Voltage_Completion(self):
        sign = 'VC'
        datas = []

        for orgId in Set.ORG_ID:
            data = self.requestData('VC1', (self.strDate, self.strDate, orgId))
            for subData in data:
                subData['orgName'] = Set.ORG_ID_DICT[subData['orgId']]
                subData['powerIn'] = subData['powerInTotal']
                subData['powerOut'] = subData['powerOutTotal']
                subData['statDate'] = self.strDate

            datas += data

            self.printInfo(self.strDate, sign, len(datas),
                           'orgId:%s; ' % (orgId))

        Id = 'd66bbf17-2310-4947-85d5-6922e3bd8ac4'

        data = self.requestData('VC2', (self.strDate, Id))
        for subData in data:
            if subData['ORG_ID'] == Set.ORG_ID[0]:
                data = []
                data.append(subData)

        data += self.requestData('VC2', (self.strDate, Set.ORG_ID[0]))

        for subData in data:
            subData['orgId'] = subData['ORG_ID']
            subData['orgName'] = subData['ORG_NAME']
            subData['voltLevel'] = '交流380伏'
            subData['rateLoss'] = subData['RATE_LOSS']
            subData['powerLoss'] = subData['LOSS_POWER']
            subData['powerSal'] = subData['SAL_POWER']
            subData['powerSup'] = subData['SUB_POWER']
            subData['powerIn'] = subData['IN_POWER']
            subData['powerOut'] = subData['OUT_POWER']
            subData['ve'] = '08'
            subData['statDate'] = subData['STAT_DATE']

        datas += data

        datas = self.Base_Rate(datas)

        self.printInfo(self.strDate, sign, len(datas))

        return self.processData(sign, datas)

    # 爬取线路完成度信息
    def Line_Completion(self):
        sign = 'LC'

        data = self.requestData(sign, (self.strDate))

        for subData in data:
            subData['orgId'] = subData['id']
            subData['orgName'] = subData['name']

        self.printInfo(self.strDate, sign, len(data))

        return self.processData(sign, data)

    # 爬取线路线损完成度信息
    def Line_Loss_Completion(self):
        sign = 'LLC'

        data = self.requestData(sign, (self.strDate))

        for subData in data:
            subData['statDate'] = subData['STAT_DATE']

        self.printInfo(self.strDate, sign, len(data))

        return self.processData(sign, data)

    # 爬取台区完成度信息
    def TG_Completion(self):
        sign = 'TGC'
        datas = []

        data = self.requestData(sign, (Set.ORG_ID[0], self.strDate, '04'))
        datas += data

        orgId = 'D3305320A16D4D1EB4A82DD6A7C75A47'
        data = self.requestData(sign, (orgId, self.strDate, '05'))
        datas += data

        for subData in datas:
            subData['orgId'] = subData['id']
            subData['orgName'] = subData['name']

        self.printInfo(self.strDate, sign, len(datas))

        return self.processData(sign, datas)

    # 爬取台区线损完成度信息
    def TG_Loss_Completion(self):
        sign = 'TGLC'
        datas = []

        data = self.requestData(sign, (self.strDate, Set.ORG_ID[0]))
        datas += data

        orgId = 'D3305320A16D4D1EB4A82DD6A7C75A47'
        data = self.requestData(sign, (self.strDate, orgId))
        datas += data

        for subData in datas:
            subData['statDate'] = subData['STAT_DATE']

        self.printInfo(self.strDate, sign, len(datas))

        return self.processData(sign, datas)

    # 爬取高压表底完整率完成情况
    def Integrity_Rate_Completion(self):
        sign = 'IRC'

        data = self.requestData(sign, (self.strLastDate))

        for subData in data:
            subData['statDate'] = self.strLastDate

        self.printInfo(self.strLastDate, sign, len(data))

        return self.processData(sign, data)

    # 爬取台区电流值
    def TG_Electric_Current(self):
        sign = 'TGEC'
        datas = []
        tgs = self.selectTGSQL()

        for tg in tgs:
            data = self.requestData(sign, (self.strDate, Set.ORG_ID[0], tg[0]))

            if len(data) == 0:
                self.printInfo(self.strDate, sign, len(datas),
                               'tgId:%s; isNULL; ' % (tg[0]))
                continue

            for subData in data:
                datas += self.processData(sign, subData, tg[0])

            self.printInfo(self.strDate, sign, len(datas),
                           'tgId:%s; ' % (tg[0]))

        return datas

    # 爬取台区电压值
    def TG_Electric_Voltage(self):
        sign = 'TGEV'
        datas = []
        tgs = self.selectTGSQL()

        for tg in tgs:
            data = self.requestData(sign, (self.strDate, Set.ORG_ID[0], tg[0]))

            if len(data) == 0:
                self.printInfo(self.strDate, sign, len(datas),
                               'tgId:%s; isNULL; ' % (tg[0]))
                continue

            for subData in data:
                datas += self.processData(sign, subData, tg[0])

            self.printInfo(self.strDate, sign, len(datas),
                           'tgId:%s; ' % (tg[0]))

        return datas

    # 爬取台区功率因数
    def TG_Power_Curve(self):
        sign = 'TGPC'
        datas = []
        tgs = self.selectTGSQL()

        for tg in tgs:
            data = self.requestData(sign, (self.strDate, Set.ORG_ID[0], tg[0]))

            if len(data) == 0:
                self.printInfo(self.strDate, sign, len(datas),
                               'tgId:%s; isNULL; ' % (tg[0]))
                continue

            for subData in data:
                datas += self.processData(sign, subData, tg[0])

            self.printInfo(self.strDate, sign, len(datas),
                           'tgId:%s; ' % (tg[0]))

        return datas

    # 爬取台区功率因数
    def TG_Power_Factor(self):
        sign = 'TGPF'
        datas = []
        tgs = self.selectTGSQL()

        for tg in tgs:
            data = self.requestData(sign, (self.strDate, Set.ORG_ID[0], tg[0]))

            if len(data) == 0:
                self.printInfo(self.strDate, sign, len(datas),
                               'tgId:%s; isNULL; ' % (tg[0]))
                continue
            # print(data)
            for subData in data:
                datas += self.processData(sign, subData, tg[0])

            self.printInfo(self.strDate, sign, len(datas),
                           'tgId:%s; ' % (tg[0]))
            # if '941527835' == tg[0]:
            #     break
        return datas

    # 爬取用户电表码值信息
    def User_Table_Value(self):
        sign = 'UTV'
        datas = []
        tgs = self.selectTGSQL()
        for tg in tgs:
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (tg[0], self.strDate, page, Set.PAGE_SIZE))
                for subData in data:
                    subData["tgId"] = tg[0]
                    subData["statDate"] = self.strDate

                datas += data

                self.printInfo(self.strDate, sign, len(datas),
                               'tgId:%s; page:%s; ' % (tg[0], page))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 执行爬取
    def implement(self):
        if not self.isExist('RC'):
            RC = self.Region_Completion()
            self.insertSQL(Set.TABLE_NAME['RC'], Set.TABLE['RC'], RC)

        if not self.isExist('VC'):
            VC = self.Voltage_Completion()
            self.insertSQL(Set.TABLE_NAME['VC'], Set.TABLE['VC'], VC)

        if not self.isExist('LC'):
            LC = self.Line_Completion()
            self.insertSQL(Set.TABLE_NAME['LC'], Set.TABLE['LC'], LC)

        if not self.isExist('LLC'):
            LLC = self.Line_Loss_Completion()
            self.insertSQL(Set.TABLE_NAME['LLC'], Set.TABLE['LLC'], LLC)

        if not self.isExist('TGC'):
            TGC = self.TG_Completion()
            self.insertSQL(Set.TABLE_NAME['TGC'], Set.TABLE['TGC'], TGC)

        if not self.isExist('TGLC'):
            TGLC = self.TG_Loss_Completion()
            self.insertSQL(Set.TABLE_NAME['TGLC'], Set.TABLE['TGLC'], TGLC)

        if not self.isExist('IRC'):
            IRC = self.Integrity_Rate_Completion()
            self.insertSQL(Set.TABLE_NAME['IRC'], Set.TABLE['IRC'], IRC)

        if not self.isExist('TGAI'):
            TGAI = self.TG_Abnormal_Info()
            self.insertSQL(Set.TABLE_NAME['TGAI'], Set.TABLE['TGAI'], TGAI)

        if not self.isExist('LAI'):
            LAI = self.Line_Abnormal_Info()
            self.insertSQL(Set.TABLE_NAME['LAI'], Set.TABLE['LAI'], LAI)

        if not self.isExist('LPI'):
            LPI = self.Line_Power_Info()
            self.insertSQL(Set.TABLE_NAME['LPI'], Set.TABLE['LPI'], LPI)

        if not self.isExist('GPI'):
            GPI = self.Gateway_Power_Info()
            self.insertSQL(Set.TABLE_NAME['GPI'], Set.TABLE['GPI'], GPI)

        if not self.isExist('TGPI'):
            TGPI = self.TG_Power_Info()
            self.insertSQL(Set.TABLE_NAME['TGPI'], Set.TABLE['TGPI'], TGPI)

        if not self.isExist('TGTV'):
            TGTV = self.TG_Table_Value()
            self.insertSQL(Set.TABLE_NAME['TGTV'], Set.TABLE['TGTV'], TGTV)

        if not self.isExist('TGEC'):
            TGEC = self.TG_Electric_Current()
            self.insertSQL(Set.TABLE_NAME['TGEC'], Set.TABLE['TGEC'], TGEC)

        if not self.isExist('TGEV'):
            TGEV = self.TG_Electric_Voltage()
            self.insertSQL(Set.TABLE_NAME['TGEV'], Set.TABLE['TGEV'], TGEV)

        if not self.isExist('TGPC'):
            TGPC = self.TG_Power_Curve()
            self.insertSQL(Set.TABLE_NAME['TGPC'], Set.TABLE['TGPC'], TGPC)

        if not self.isExist('TGPF'):
            TGPF = self.TG_Power_Factor()
            self.insertSQL(Set.TABLE_NAME['TGPF'], Set.TABLE['TGPF'], TGPF)

        if not self.isExist('UTV'):
            UTV = self.User_Table_Value()
            self.insertSQL(Set.TABLE_NAME['UTV'], Set.TABLE['UTV'], UTV)

    def __del__(self):
        self.cur.close()
        self.coon.close()
        self.file.close()
