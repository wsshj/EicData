import os
import requests
import time
import datetime
import json
import pymysql

import settings as Set
from base import Base


class Spider(Base):
    def __init__(self, date):
        Base.__init__(self, date)
        self.session = self.login()

    # 数据插入对应表
    def insertSQL(self, sgin, datas, tableDate=''):
        tableName = Set.TABLE_NAME[sgin] + tableDate
        colunm = Set.TABLE[sgin]

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

    # 如果表不存在则创建表
    def createTable(self, tableName, sourceTable):
        sql = ' SELECT count(*) AS count \
                FROM information_schema.tables \
                WHERE table_name="%s";' % (tableName)

        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return False

        if res[0][0] == 0:
            sql = "create table %s LIKE %s" % (tableName, sourceTable)

            try:
                self.cur.execute(sql)
                self.coon.commit()
                self.printResult(
                    "table(%s):Table create successfully." % tableName,
                    "succ:  ")
            except Exception as e:
                self.printResult(e, "error:  ")
                self.coon.rollback()
                return False

        return True

    # 通过与前一日数据量比对，判断数据爬取是否成功
    def isEnoughData(self, tableName, dataNum):
        if tableName == 'line_abnormal_info' or tableName == 'tg_abnormal_info':
            return True

        sql = " SELECT statDate \
                FROM %s \
                WHERE statDate='%s';" % (tableName, self.strLastDay)

        try:
            lastNum = self.cur.execute(sql)
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return False

        if lastNum == 0:
            return True

        cRate = round((dataNum - lastNum) / lastNum, 3)
        self.printResult(
            "table(%s):The number of data is %s;" % (tableName, cRate),
            "tips:  ")

        if cRate >= -0.050:
            return True
        else:
            return False

    # 查询线路表中当日数据的线路ID和线路Type
    def selectLineSQL(self, dateType='d'):
        if dateType == 'd':
            sql = " SELECT lineId,lineNo,lineName,lineType \
                    FROM line_power_info \
                    WHERE statDate='%s';" % self.strDay
        elif dateType == 'm':
            sql = " SELECT lineId,lineNo,lineName,lineType \
                    FROM line_power_month_info \
                    WHERE statDate='%s';" % self.strLastMonth

        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return ()
        else:
            return res

    # 查询北一区台区的TgId
    def selectTGSQL(self):
        sql = "SELECT tgId FROM tg_byq;"

        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return ()
        else:
            return res

    # 查询北一区高压用户的consTgId
    def selectHVUSQL(self):
        sql = "SELECT consTgId FROM hvu_byq;"

        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return ()
        else:
            return res

    # 判断是否已存在当天数据
    def isExist(self, sign, tableDate=''):
        tableName = Set.TABLE_NAME[sign] + tableDate
        sourceTable = Set.TABLE_NAME[sign]
    
        if sign == 'IRC':
            strDate = self.strLastDay
        elif sign in Set.SIGN_M:
            strDate = self.strLastMonth
        else:
            strDate = self.strDay

        if tableName != sourceTable:
            self.createTable(tableName, sourceTable)

        sql = "SELECT statDate FROM %s WHERE statDate='%s';" % (
            tableName, strDate)

        try:
            num = self.cur.execute(sql)
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            num = -1

        if num < 0:
            print("\n数据类型：%s；数据日期：%s；状态：数据查询错误；" % (Set.SIGN[sign], strDate))
            return True
        elif num > 0:
            print("\n数据类型：%s；数据日期：%s；数据量：%s；状态：已执行；" %
                  (Set.SIGN[sign], strDate, num))
            return True
        else:
            print("\n数据类型：%s；状态：未执行；开始执行..." % (Set.SIGN[sign]))
            return False

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
                if 'TG' in sign:
                    params = {
                        'statDate': tupleData[0],
                        'orgId': tupleData[1],
                        'tgId': tupleData[2],
                        'meterId': 'null'
                    }
                elif 'HVU' in sign:
                    params = {
                        'statDate': tupleData[0],
                        'orgId': tupleData[1],
                        'consId': tupleData[2],
                        'meterId': 'null'
                    }
                else:
                    params = {}

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
                if type(datas) == str:
                    break
                if len(datas[data]) == 0:
                    break
                value = []
                value.append(tgId)
                value.append(self.strDay)
                if 'EC' in sign or 'EV' in sign:
                    value.append(data)
                if len(datas[data]) < 24:
                    for i in range(24 - len(datas[data])):
                        value.append('0')
                value += datas[data]

                values.append(tuple(value))

        return values

    # 台区和高压用户电流，电压，功率因数，功率曲线统一爬取方法
    def TGHVU_CVPF_Info(self, sign, consTgIds):
        datas = []

        for consTgId in consTgIds:
            data = self.requestData(sign, (self.strDay, Set.ORG_ID[0], consTgId[0]))

            if len(data) == 0:
                self.printInfo(self.strDay, sign, len(datas),
                               'consTgId:%s; isNULL; ' % (consTgId[0]))
                continue
            
            for subData in data:
                datas += self.processData(sign, subData, consTgId[0])

            self.printInfo(self.strDay, sign, len(datas),
                           'consTgId:%s; ' % (consTgId[0]))

        return datas

    # 爬取台区异常信息
    def TG_Abnormal_Info(self):
        sign = 'TGAI'
        datas = []

        for Type in Set.ABNORMAL_TYPE:
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (self.strDay, Type, page, Set.PAGE_SIZE))
                for subData in data:
                    subData['ABNORMAL_TYPE'] = Type
                    subData['statDate'] = subData['STAT_DATE']

                datas += data

                self.printInfo(self.strDay, sign, len(datas),
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
                    sign, (Type, self.strDay, page, Set.PAGE_SIZE))
                for subData in data:
                    subData['ABNORMAL_TYPE'] = Type
                    subData['statDate'] = subData['STAT_DATE']

                datas += data

                self.printInfo(self.strDay, sign, len(datas),
                               'Type:%s; page:%s; ' % (Type, page))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 爬取线路电量信息
    def Line_Power_Info(self):
        sign = 'LPI'
        datas = []

        for page in range(1, 1000):
            data = self.requestData(sign, (self.strDay, page, Set.PAGE_SIZE))
            datas += data

            self.printInfo(self.strDay, sign, len(datas),
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
                    sign, (orgId, self.strDay, page, Set.PAGE_SIZE))

                for subData in data:
                    subData["outIn"] = subData['inOut']
                    subData["statDate"] = subData['dataDate']

                datas += data

                self.printInfo(self.strDay, sign, len(datas),
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
            data = self.requestData(sign, (line[0], self.strDay))

            for subData in data:
                subData["lineId"] = line[0]

            datas += data

            self.printInfo(self.strDay, sign, len(datas),
                           'lineId:%s; ' % (line[0]))

        return self.processData(sign, datas)

    # 爬取台区和专变（高压用户）电表码值信息
    def TG_Table_Value(self):
        sign = 'TGTV'
        datas = []
        lines = self.selectLineSQL()

        for line in lines:
            data = self.requestData(sign, (line[0], self.strDay, line[3]))

            for subData in data:
                subData["PBLineId"] = line[0]
                subData["PBLineNo"] = line[1]
                subData["PBLineName"] = line[2]
                subData["PBLineType"] = line[3]

            datas += data

            self.printInfo(self.strDay, sign, len(datas),
                           'lineId:%s; lineType:%s; ' % (line[0], line[3]))

        return self.processData(sign, datas)

    # 爬取分区域完成度
    def Region_Completion(self):
        sign = 'RC'
        datas = []

        for orgId in Set.ORG_ID:
            if orgId == '1C3DAEFA42964361BCF73D24F19127F9':
                data = self.requestData(
                    'RC1', (self.strDay, self.strDay, orgId, '04'))
            else:
                data = self.requestData(
                    'RC1', (self.strDay, self.strDay, orgId, '05'))

            datas += data

            self.printInfo(self.strDay, sign, len(datas))

        data = self.requestData('RC2', (Set.ORG_ID[0]))

        for subDatas in datas:
            subDatas['statDate'] = subDatas['STAT_DATE']
            for subData in data:
                if subDatas['ORG_ID'] == subData['ORG_ID']:
                    subDatas['RATE_LOSS'] = subData['RATE_LOSS']
                    subDatas['STAND_PERCE'] = subData['STAND_PERCE']

        self.printInfo(self.strDay, sign, len(datas))

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
        self.printInfo(self.strDay, sign, len(datas), 'voltLevel:220kv; ')

        data110 = self.requestData('VC3', ('32', Set.ORG_ID[0]))
        self.printInfo(self.strDay, sign, len(datas), 'voltLevel:110kv; ')

        data35 = self.requestData('VC3', ('25', Set.ORG_ID[0]))
        self.printInfo(self.strDay, sign, len(datas), 'voltLevel:35kv; ')

        data10 = self.requestData('VC4', (strLastMonth))
        self.printInfo(self.strDay, sign, len(datas), 'voltLevel:10kv; ')

        data380 = self.requestData('VC5', (strLastMonth))
        self.printInfo(self.strDay, sign, len(datas), 'voltLevel:380v; ')

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
            data = self.requestData('VC1', (self.strDay, self.strDay, orgId))
            for subData in data:
                subData['orgName'] = Set.ORG_ID_DICT[subData['orgId']]
                subData['powerIn'] = subData['powerInTotal']
                subData['powerOut'] = subData['powerOutTotal']
                subData['statDate'] = self.strDay

            datas += data

            self.printInfo(self.strDay, sign, len(datas),
                           'orgId:%s; ' % (orgId))

        Id = 'd66bbf17-2310-4947-85d5-6922e3bd8ac4'

        data = self.requestData('VC2', (self.strDay, Id))
        for subData in data:
            if subData['ORG_ID'] == Set.ORG_ID[0]:
                data = []
                data.append(subData)

        data += self.requestData('VC2', (self.strDay, Set.ORG_ID[0]))

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

        self.printInfo(self.strDay, sign, len(datas))

        return self.processData(sign, datas)

    # 爬取线路完成度信息
    def Line_Completion(self):
        sign = 'LC'

        data = self.requestData(sign, (self.strDay))

        for subData in data:
            subData['orgId'] = subData['id']
            subData['orgName'] = subData['name']

        self.printInfo(self.strDay, sign, len(data))

        return self.processData(sign, data)

    # 爬取线路线损完成度信息
    def Line_Loss_Completion(self):
        sign = 'LLC'

        data = self.requestData(sign, (self.strDay))

        for subData in data:
            subData['statDate'] = subData['STAT_DATE']

        self.printInfo(self.strDay, sign, len(data))

        return self.processData(sign, data)

    # 爬取台区完成度信息
    def TG_Completion(self):
        sign = 'TGC'
        datas = []

        data = self.requestData(sign, (Set.ORG_ID[0], self.strDay, '04'))
        datas += data

        orgId = 'D3305320A16D4D1EB4A82DD6A7C75A47'
        data = self.requestData(sign, (orgId, self.strDay, '05'))
        datas += data

        for subData in datas:
            subData['orgId'] = subData['id']
            subData['orgName'] = subData['name']

        self.printInfo(self.strDay, sign, len(datas))

        return self.processData(sign, datas)

    # 爬取台区线损完成度信息
    def TG_Loss_Completion(self):
        sign = 'TGLC'
        datas = []

        data = self.requestData(sign, (self.strDay, Set.ORG_ID[0]))
        datas += data

        orgId = 'D3305320A16D4D1EB4A82DD6A7C75A47'
        data = self.requestData(sign, (self.strDay, orgId))
        datas += data

        for subData in datas:
            subData['statDate'] = subData['STAT_DATE']

        self.printInfo(self.strDay, sign, len(datas))

        return self.processData(sign, datas)

    # 爬取高压表底完整率完成情况
    def Integrity_Rate_Completion(self):
        sign = 'IRC'

        data = self.requestData(sign, (self.strLastDay))

        for subData in data:
            subData['statDate'] = self.strLastDay

        self.printInfo(self.strLastDay, sign, len(data))

        return self.processData(sign, data)

    # 爬取台区电流值
    def TG_Electric_Current(self):
        sign = 'TGEC'

        tgs = self.selectTGSQL()

        return self.TGHVU_CVPF_Info(sign, tgs)

    # 爬取台区电压值
    def TG_Electric_Voltage(self):
        sign = 'TGEV'

        tgs = self.selectTGSQL()

        return self.TGHVU_CVPF_Info(sign, tgs)

    # 爬取台区功率因数
    def TG_Power_Curve(self):
        sign = 'TGPC'

        tgs = self.selectTGSQL()

        return self.TGHVU_CVPF_Info(sign, tgs)

    # 爬取台区功率因数
    def TG_Power_Factor(self):
        sign = 'TGPF'

        tgs = self.selectTGSQL()

        return self.TGHVU_CVPF_Info(sign, tgs)

    # 爬取高压用户电流值
    def HVU_Electric_Current(self):
        sign = 'HVUEC'

        consIds = self.selectHVUSQL()

        return self.TGHVU_CVPF_Info(sign, consIds)

    # 爬取高压用户电压值
    def HVU_Electric_Voltage(self):
        sign = 'HVUEV'

        consIds = self.selectHVUSQL()

        return self.TGHVU_CVPF_Info(sign, consIds)

    # 爬取高压用户功率因数
    def HVU_Power_Curve(self):
        sign = 'HVUPC'

        consIds = self.selectHVUSQL()

        return self.TGHVU_CVPF_Info(sign, consIds)

    # 爬取高压用户功率因数
    def HVU_Power_Factor(self):
        sign = 'HVUPF'

        consIds = self.selectHVUSQL()

        return self.TGHVU_CVPF_Info(sign, consIds)

    # 爬取用户电表码值信息
    def User_Table_Value(self):
        sign = 'UTV'
        datas = []
        tgs = self.selectTGSQL()
        for tg in tgs:
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (tg[0], self.strDay, page, Set.PAGE_SIZE))
                for subData in data:
                    subData["tgId"] = tg[0]
                    subData["statDate"] = self.strDay

                datas += data

                self.printInfo(self.strDay, sign, len(datas),
                               'tgId:%s; page:%s; ' % (tg[0], page))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 爬取线路电量信息（月）
    def Line_Power_Month_Info(self):
        sign = 'LPMI'
        datas = []

        for page in range(1, 1000):
            data = self.requestData(sign, (self.strLastMonth, page, Set.PAGE_SIZE))
            datas += data

            self.printInfo(self.strLastMonth, sign, len(datas),
                           'page:%s; ' % (page))

            if len(data) < Set.PAGE_SIZE:
                break

        return self.processData(sign, datas)

    # 爬取线路关口电量信息（月）
    def Gateway_Power_Month_Info(self):
        sign = 'GPMI'
        datas = []

        for orgId in Set.ORG_ID:
            if orgId == Set.ORG_ID[0]:
                continue
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (orgId, self.strLastMonth, page, Set.PAGE_SIZE))

                for subData in data:
                    subData["outIn"] = subData['inOut']
                    subData["statDate"] = subData['dataDate']

                datas += data

                self.printInfo(self.strLastMonth, sign, len(datas),
                               'page:%s; orgId:%s; ' % (page, orgId))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 爬取台区电量信息（月）
    def TG_Power_Month_Info(self):
        sign = 'TGPMI'
        datas = []
        lines = self.selectLineSQL('m')

        for line in lines:
            data = self.requestData(sign, (line[0], self.strLastMonth))

            for subData in data:
                subData["lineId"] = line[0]

            datas += data

            self.printInfo(self.strLastMonth, sign, len(datas),
                           'lineId:%s; ' % (line[0]))

        return self.processData(sign, datas)

    # 爬取台区和专变（高压用户）电表码值信息（月）
    def TG_Table_Month_Value(self):
        sign = 'TGTMV'
        datas = []
        lines = self.selectLineSQL('m')

        for line in lines:
            data = self.requestData(sign, (line[0], self.strLastMonth, line[3]))

            for subData in data:
                subData["PBLineId"] = line[0]
                subData["PBLineNo"] = line[1]
                subData["PBLineName"] = line[2]
                subData["PBLineType"] = line[3]
                subData["statDate"] = self.strLastMonth

            datas += data

            self.printInfo(self.strLastMonth, sign, len(datas),
                           'lineId:%s; lineType:%s; ' % (line[0], line[3]))

        return self.processData(sign, datas)

    # 爬取用户电表码值信息（月）
    def User_Table_Month_Value(self):
        sign = 'UTMV'
        datas = []
        tgs = self.selectTGSQL()
        for tg in tgs:
            for page in range(1, 1000):
                data = self.requestData(
                    sign, (tg[0], self.strLastMonth, page, Set.PAGE_SIZE))
                for subData in data:
                    subData["tgId"] = tg[0]
                    subData["statDate"] = self.strLastMonth

                datas += data

                self.printInfo(self.strLastMonth, sign, len(datas),
                               'tgId:%s; page:%s; ' % (tg[0], page))

                if len(data) < Set.PAGE_SIZE:
                    break

        return self.processData(sign, datas)

    # 爬取线路基础信息
    def Line_Base_Info(self):
        sign = 'LBI'
        datas = []

        for page in range(1, 1000):
            data = self.requestData(sign, (page, Set.PAGE_SIZE))

            for subData in data:
                subData["statDate"] = self.strLastMonth

            datas += data

            self.printInfo(self.strLastMonth, sign, len(datas),
                           'page:%s; ' % (page))

            if len(data) < Set.PAGE_SIZE:
                break

        return self.processData(sign, datas)

    # 爬取台区基础信息
    def TG_Base_Info(self):
        sign = 'TGBI'
        datas = []

        for page in range(1, 1000):
            data = self.requestData(sign, (page, Set.PAGE_SIZE))
            
            for subData in data:
                subData["statDate"] = self.strLastMonth

            datas += data

            self.printInfo(self.strLastMonth, sign, len(datas),
                           'page:%s; ' % (page))

            if len(data) < Set.PAGE_SIZE:
                break

        return self.processData(sign, datas)

    # 执行爬取
    def implement(self):
        if not self.isExist('RC'):
            RC = self.Region_Completion()
            self.insertSQL('RC', RC)

        if not self.isExist('VC'):
            VC = self.Voltage_Completion()
            self.insertSQL('VC', VC)

        if not self.isExist('LC'):
            LC = self.Line_Completion()
            self.insertSQL('LC', LC)

        if not self.isExist('LLC'):
            LLC = self.Line_Loss_Completion()
            self.insertSQL('LLC', LLC)

        if not self.isExist('TGC'):
            TGC = self.TG_Completion()
            self.insertSQL('TGC', TGC)

        if not self.isExist('TGLC'):
            TGLC = self.TG_Loss_Completion()
            self.insertSQL('TGLC', TGLC)

        if not self.isExist('IRC'):
            IRC = self.Integrity_Rate_Completion()
            self.insertSQL('IRC', IRC)

        if not self.isExist('TGAI'):
            TGAI = self.TG_Abnormal_Info()
            self.insertSQL('TGAI', TGAI)

        if not self.isExist('LAI'):
            LAI = self.Line_Abnormal_Info()
            self.insertSQL('LAI', LAI)

        if not self.isExist('LPI'):
            LPI = self.Line_Power_Info()
            self.insertSQL('LPI', LPI)

        if not self.isExist('GPI'):
            GPI = self.Gateway_Power_Info()
            self.insertSQL('GPI', GPI)

        if not self.isExist('TGPI'):
            TGPI = self.TG_Power_Info()
            self.insertSQL('TGPI', TGPI)

        if not self.isExist('TGTV'):
            TGTV = self.TG_Table_Value()
            self.insertSQL('TGTV', TGTV)

        if not self.isExist('TGEC'):
            TGEC = self.TG_Electric_Current()
            self.insertSQL('TGEC', TGEC)

        if not self.isExist('TGEV'):
            TGEV = self.TG_Electric_Voltage()
            self.insertSQL('TGEV', TGEV)

        if not self.isExist('TGPC'):
            TGPC = self.TG_Power_Curve()
            self.insertSQL('TGPC', TGPC)

        if not self.isExist('TGPF'):
            TGPF = self.TG_Power_Factor()
            self.insertSQL('TGPF', TGPF)

        if not self.isExist('HVUEC'):
            HVUEC = self.HVU_Electric_Current()
            self.insertSQL('HVUEC', HVUEC)

        if not self.isExist('HVUEV'):
            HVUEV = self.HVU_Electric_Voltage()
            self.insertSQL('HVUEV', HVUEV)

        if not self.isExist('HVUPC'):
            HVUPC = self.HVU_Power_Curve()
            self.insertSQL('HVUPC', HVUPC)

        if not self.isExist('HVUPF'):
            HVUPF = self.HVU_Power_Factor()
            self.insertSQL('HVUPF', HVUPF)

        if not self.isExist('UTV', self.strTableMonth):
            UTV = self.User_Table_Value()
            self.insertSQL('UTV', UTV, self.strTableMonth)

        if int(datetime.datetime.now().strftime('%d')) > 7:
            if not self.isExist('LPMI'):
                LPMI = self.Line_Power_Month_Info()
                self.insertSQL('LPMI', LPMI)

            if not self.isExist('GPMI'):
                GPMI = self.Gateway_Power_Month_Info()
                self.insertSQL('GPMI', GPMI)

            if not self.isExist('TGPMI'):
                TGPMI = self.TG_Power_Month_Info()
                self.insertSQL('TGPMI', TGPMI)

            if not self.isExist('TGTMV'):
                TGTMV = self.TG_Table_Month_Value()
                self.insertSQL('TGTMV', TGTMV)

            if not self.isExist('UTMV'):
                UTMV = self.User_Table_Month_Value()
                self.insertSQL('UTMV', UTMV)

            # if not self.isExist('LBI'):
            #     LBI = self.Line_Base_Info()
            #     self.insertSQL('LBI', LBI)

            # if not self.isExist('TGBI'):
            #     TGBI = self.TG_Base_Info()
            #     self.insertSQL('TGBI', TGBI)

    def __del__(self):
        Base.__del__(self)
