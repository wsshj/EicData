import os
import time
import datetime
import pymysql

import settings as Set


class Process:
    def __init__(self, date):
        self.strDate = date.strftime('%Y-%m-%d')
        self.strLastDate = (date -
                            datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        self.coon, self.cur = self.connectSQL()
        self.file = self.openFile()

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
        print(self.strDate)
        filePath = 'log\\process-%s.txt' % (self.strDate)
        print(filePath)
        try:
            f = open(filePath, 'a', encoding='utf-8')
        except IOError as e:
            print(e)
        else:
            return f

    # 写日志文件
    def writeFile(self, info, level=''):
        try:
            localTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            self.file.write('%swriteTime:%s; %s\n' % (level, localTime, info))
        except Exception as e:
            print(e)

    # 输出日志文件,结果信息
    def printResult(self, info, level):
        self.writeFile(info, level)
        print(info)

    # 处理字段
    def operationField(self, field):
        strField = '{a}'.format(a=field)
        strField = strField.replace("(", "")
        strField = strField.replace(")", "")
        strField = strField.replace("\'", "")

        return strField

    # 读取数据
    def dateData(self, field, tableName, date=''):
        datas = []

        strField = self.operationField(field)

        if date == '':
            command = " SELECT %s \
                        FROM %s;" % (strField, tableName)
        else:
            command = " SELECT %s \
                        FROM %s \
                        WHERE statDATE='%s';" % (strField, tableName, date)

        try:
            self.cur.execute(command)
            result = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return ()
        else:
            for row in result:
                Dict = {}
                for i in range(len(field)):
                    Dict[field[i]] = row[i]
                    if field[i] == 'statDate':
                        Dict[field[i]] = row[i].strftime('%Y-%m-%d')

                datas.append(Dict)

            return datas
    
    # 读取异常数据
    def abnormalDatas(self, sign, date):
        datas = []

        if sign == 'tg':
            command = " SELECT pi.lineId, \
                        COUNT(haveValue!=1 or null) AS abVal, \
                        COUNT(abnormalType!=0 or null) AS abLoss, \
                        COUNT(abnormalSal!=0 or null) AS abSal, \
                        COUNT(abnormalSup!=0 or null) AS abSup, \
                        COUNT(mpZxAbnormal!=0 or null) AS abZx, \
                        COUNT(mpFxAbnormal!=0 or null) AS abFx \
                        FROM tg_power_info AS pi  \
                        INNER JOIN tg_table_value AS tv \
                        ON pi.tgId=tv.consTgId  \
                        AND pi.statDate=tv.statDate \
                        WHERE pi.statDate='%s' \
                        GROUP BY pi.lineId;" % (date)
        elif sign == 'gate':
            command = " SELECT lineId, \
                        COUNT(haveValue!=1 or null) AS abVal, \
                        COUNT(zxAbnormal!=0 or null) AS abZx, \
                        COUNT(fxAbnormal!=0 or null) AS abFx \
                        FROM gateway_power_info \
                        WHERE statDate='%s' \
                        GROUP BY lineId;" % (date)
        elif sign == 'hv':
            command = " SELECT PBLineId, \
                        COUNT(mpZxAbnormal!=0 or null) AS abZx, \
                        COUNT(mpFxAbnormal!=0 or null) AS abFx, \
                        COUNT(haveValue!=1 or null) AS abVal \
                        FROM tg_table_value \
                        WHERE statDate='%s' \
                        AND consType='02' \
                        GROUP BY PBLineId;" % (date)
        else:
            return datas

        try:
            self.cur.execute(command)
            result = self.cur.fetchall()
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()
            return ()
        else:
            for row in result:
                Dict = {}
                Dict['lineId'] = row[0]
                Dict['abVal'] = row[1]
                if sign == 'tg':
                    Dict['abLoss'] = row[2]
                    Dict['abEle'] = int(row[3]) + int(row[4]) + int(row[5]) + int(row[6])
                else:
                    Dict['abEle'] = int(row[2]) + int(row[3])

                datas.append(Dict)

            return datas

    # 计算电量突增，突减
    def calculation(self, now, last):
        try:
            now = float(now)
        except Exception:
            now = 0

        try:
            last = float(last)
        except Exception:
            last = 0

        if last == 0 and now == 0:
            return 0
        elif last == 0:
            return 2

        if (now - last) / last > 0.30:
            return 2
        elif (now - last) / last < -0.30:
            return 1
        else:
            return 0

    # 线损类型转化为数字
    def change(self, aType):
        if aType == 'fs':
            return 1
        elif aType == 'gs':
            return 2
        elif aType == 'bks':
            return 3
        else:
            return 0

    # 字符处理
    def value(self, fieldName, data):
        if data.get(fieldName, '0') == '':
            return 0
        else:
            return data.get(fieldName, '0')

    # 最终修改表中数据
    def modData(self, tableName, fieldName, datas):
        rows = []

        for data in datas:
            row = "SELECT %s AS id, %s AS %s UNION" % (
                data['id'], self.value(fieldName, data), fieldName)
            rows.append(row)

        strRows = str(tuple(rows))

        strRows = strRows.replace("\'", "")
        strRows = strRows.replace("UNION,", "!")
        strRows = strRows.replace("UNION", "")
        strRows = strRows.replace("!", "UNION")

        command = "UPDATE %s a JOIN %s b USING(id) SET a.%s=b.%s;" % (
            tableName, strRows, fieldName, fieldName)

        try:
            self.cur.execute(command)
            self.coon.commit()
            self.printResult(
                'date: %s; tableName: %s; fieldName: %s;' %
                (self.strDate, tableName, fieldName), 'succ:  ')
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()

    # 台区电流数据算法
    def electricCur(self, tgId, datas):
        curs = []
        for data in datas:
            if tgId == data['consTgId']:
                curs.append(data)

            if len(curs) == 3:
                break

        for cur in curs:
            for key, value in cur.items():
                if key == '':
                    break

        return 0

    # 台区电压数据算法
    def electricVol(self, tgId, datas):
        vols = []
        ua = 0
        ub = 0
        uc = 0

        for data in datas:
            if tgId == data['consTgId']:
                vols.append(data)

            if len(vols) == 3:
                break

        for vol in vols:
            for key, value in vol.items():
                if key == 'consTgId' or key == 'threePhase':
                    continue

                if vol['threePhase'] == 'ua':
                    ua += value
                elif vol['threePhase'] == 'ub':
                    ub += value
                elif vol['threePhase'] == 'uc':
                    uc += value

        return 0

    # 台区功率因数算法
    def powerFactor(self, tgId, datas):
        factors = ()
        rate = 0

        for data in datas:
            if tgId == data['consTgId']:
                factors = data
                break

        if len(factors) == 0:
            return 0

        for key, value in factors.items():
            if key == 'consTgId' or key == 'threePhase':
                continue

            try:
                value = float(value)
            except Exception:
                value = 0

            if value < 80:
                rate += 1

        if rate > 5:
            return 1
        else:
            return 0

    # 台区过载算法
    def overload(self, tgId, pDatas, bDatas):
        powers = ()
        heavyload = 0
        overload = 0

        for bData in bDatas:
            if tgId == bData['tgId']:
                cap = bData['tgCap']
                break

        try:
            cap = float(cap)
        except Exception:
            cap = 0

        for pData in pDatas:
            if tgId == pData['consTgId']:
                powers = pData
                break

        if len(powers) == 0:
            return 0

        for key, power in powers.items():
            if key == 'consTgId' or key == 'threePhase':
                continue

            try:
                power = float(power)
            except Exception:
                power = 0

            if power > cap:
                overload += 1
                heavyload += 1
            elif power > (cap * 0.8):
                heavyload += 1

        if overload > 5:
            return 2
        elif heavyload > 5:
            return 1
        else:
            return 0

    # 线路数据分析
    def lineData(self):
        AIfield = (
            'LINE_ID',
            'MERGE_LINE_ID',
            'ABNORMAL_TYPE',
        )

        PIfield = (
            'id',
            'lineId',
            'powerSup',
            'powerSal',
        )
        # 异常数据
        aDatas = self.dateData(AIfield, 'line_abnormal_info', self.strDate)
        # 昨日数据
        yDatas = self.dateData(PIfield, 'line_power_info', self.strLastDate)
        # 今日数据
        tDatas = self.dateData(PIfield, 'line_power_info', self.strDate)
        # 异常台区数量数据
        tgDatas = self.abnormalDatas('tg', self.strDate)
        # 异常关口数量数据
        gateDatas = self.abnormalDatas('gate', self.strLastDate)
        # 异常专变数量数据
        hvDatas = self.abnormalDatas('hv', self.strDate)

        for tData in tDatas:
            for yData in yDatas:
                if tData['lineId'] == yData['lineId']:
                    tData['lastPowerSup'] = yData['powerSup']
                    tData['lastPowerSal'] = yData['powerSal']
                    tData['abnormalSup'] = self.calculation(
                        tData['powerSup'], tData['lastPowerSup'])
                    tData['abnormalSal'] = self.calculation(
                        tData['powerSal'], tData['lastPowerSal'])
                    break

            for aData in aDatas:
                if not aData['MERGE_LINE_ID'] is None:
                    aData['LINE_ID'] = aData['MERGE_LINE_ID']

                if tData['lineId'] == aData['LINE_ID']:
                    tData['abnormalType'] = self.change(aData['ABNORMAL_TYPE'])
                    break

            for tgData in tgDatas:
                if tData['lineId'] == tgData['lineId']:
                    tData['pmAbLossNum'] = tgData['abLoss']
                    tData['pmAbEleNum'] = tgData['abEle']
                    tData['pmAbValNum'] = tgData['abVal']

            for gateData in gateDatas:
                if tData['lineId'] == gateData['lineId']:
                    tData['lmAbEleNum'] = gateData['abEle']
                    tData['lmAbValNum'] = gateData['abVal']

            for hvData in hvDatas:
                if tData['lineId'] == hvData['lineId']:
                    tData['primAbEleNum'] = hvData['abEle']
                    tData['primAbValNum'] = hvData['abVal']

        self.modData('line_power_info', 'lastPowerSup', tDatas)
        self.modData('line_power_info', 'lastPowerSal', tDatas)
        self.modData('line_power_info', 'abnormalSup', tDatas)
        self.modData('line_power_info', 'abnormalSal', tDatas)
        self.modData('line_power_info', 'abnormalType', tDatas)
        self.modData('line_power_info', 'pmAbLossNum', tDatas)
        self.modData('line_power_info', 'pmAbEleNum', tDatas)
        self.modData('line_power_info', 'pmAbValNum', tDatas)
        self.modData('line_power_info', 'lmAbEleNum', tDatas)
        self.modData('line_power_info', 'lmAbValNum', tDatas)
        self.modData('line_power_info', 'primAbEleNum', tDatas)
        self.modData('line_power_info', 'primAbValNum', tDatas)

    # 台区数据分析
    def tgData(self):
        AIfield = (
            'TG_ID',
            'ABNORMAL_TYPE',
        )

        PIfield = (
            'id',
            'tgId',
            'powerSup',
            'powerSal',
        )

        CVFfield = (
            'consTgId', 'threePhase', 'h0', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'h7', 'h8', 'h9', 'h10', 'h11', 'h12', 'h13', 'h14', 'h15', 'h16',
            'h17', 'h18', 'h19', 'h20', 'h21', 'h22', 'h23',
        )

        Bfield = ('tgId', 'tgCap')

        # 异常数据
        aDatas = self.dateData(AIfield, 'tg_abnormal_info', self.strDate)
        # 昨日数据
        yDatas = self.dateData(PIfield, 'tg_power_info', self.strLastDate)
        # 今日数据
        tDatas = self.dateData(PIfield, 'tg_power_info', self.strDate)
        # 台区容量数据
        bDatas = self.dateData(Bfield, 'tg_base')
        # 功率曲线算法
        pDatas = self.dateData(CVFfield, 'tg_power_curve', self.strDate)
        # 电流数据
        cDatas = self.dateData(CVFfield, 'tg_electric_current', self.strDate)
        # 电压数据
        vDatas = self.dateData(CVFfield, 'tg_electric_voltage', self.strDate)
        # 功率因数数据
        fDatas = self.dateData(CVFfield, 'tg_power_factor', self.strDate)

        for tData in tDatas:
            for yData in yDatas:
                if tData['tgId'] == yData['tgId']:
                    tData['lastPowerSup'] = yData['powerSup']
                    tData['lastPowerSal'] = yData['powerSal']
                    tData['abnormalSup'] = self.calculation(
                        tData['powerSup'], tData['lastPowerSup'])
                    tData['abnormalSal'] = self.calculation(
                        tData['powerSal'], tData['lastPowerSal'])
                    break

            for aData in aDatas:
                if tData['tgId'] == aData['TG_ID']:
                    tData['abnormalType'] = self.change(aData['ABNORMAL_TYPE'])
                    break

            tData['overload'] = self.overload(tData['tgId'], pDatas, bDatas)
            tData['powerFactor'] = self.powerFactor(tData['tgId'], fDatas)
            # tData['electricCur'] = self.electricCur(tData['tgId'], cDatas)
            # tData['electricVol'] = self.electricVol(tData['tgId'], vDatas)

        # self.modData('tg_power_info', 'lastPowerSup', tDatas)
        # self.modData('tg_power_info', 'lastPowerSal', tDatas)
        # self.modData('tg_power_info', 'abnormalSup', tDatas)
        # self.modData('tg_power_info', 'abnormalSal', tDatas)
        # self.modData('tg_power_info', 'abnormalType', tDatas)
        # self.modData('tg_power_info', 'overload', tDatas)
        self.modData('tg_power_info', 'powerFactor', tDatas)
        # self.modData('tg_power_info', 'electricCur', tDatas)
        # self.modData('tg_power_info', 'electricVol', tDatas)

    # 关口数据分析
    def gateData(self):
        PIfield = ('id', 'mpId', 'zxPower', 'fxPower', 'zxSbd')
        # 昨日数据
        yDatas = self.dateData(PIfield, 'gateway_power_info', self.strLastDate)
        # 今日数据
        tDatas = self.dateData(PIfield, 'gateway_power_info', self.strDate)

        for tData in tDatas:
            for yData in yDatas:
                if tData['mpId'] == yData['mpId']:
                    tData['zxLastPower'] = yData['zxPower']
                    tData['fxLastPower'] = yData['fxPower']
                    tData['zxAbnormal'] = self.calculation(
                        tData['zxPower'], tData['zxLastPower'])
                    tData['fxAbnormal'] = self.calculation(
                        tData['fxPower'], tData['fxLastPower'])
                    break

            if tData['zxSbd'] == '':
                tData['haveValue'] = 0
            else:
                tData['haveValue'] = 1

        self.modData('gateway_power_info', 'zxLastPower', tDatas)
        self.modData('gateway_power_info', 'fxLastPower', tDatas)
        self.modData('gateway_power_info', 'zxAbnormal', tDatas)
        self.modData('gateway_power_info', 'fxAbnormal', tDatas)
        self.modData('gateway_power_info', 'haveValue', tDatas)

    # 台区和高压用户数据分析
    def TVData(self):
        PIfield = ('id', 'mpLastZxPower', 'mpZxPower', 'mpLastFxPower',
                   'mpFxPower', 'zxsbd')
        # 今日数据
        tDatas = self.dateData(PIfield, 'tg_table_value', self.strDate)

        datas = []
        for tData in tDatas:
            tData['mpZxAbnormal'] = self.calculation(tData['mpZxPower'],
                                                     tData['mpLastZxPower'])
            tData['mpFxAbnormal'] = self.calculation(tData['mpFxPower'],
                                                     tData['mpLastFxPower'])

            if tData['zxsbd'] == '':
                tData['haveValue'] = 0
            else:
                tData['haveValue'] = 1

            if tData['mpZxAbnormal'] != 0 or tData[
                    'mpZxAbnormal'] != 0 or tData['haveValue'] != 1:
                datas.append(tData)

        self.modData('tg_table_value', 'mpZxAbnormal', datas)
        self.modData('tg_table_value', 'mpFxAbnormal', datas)
        self.modData('tg_table_value', 'haveValue', datas)

    # 用户数据分析(窃电分析)
    def UTVData(self):
        PIfield = ('id', 'lastPeriod')
        # 今日数据
        tDatas = self.dateData(PIfield, 'user_table_value', self.strDate)

        datas = []
        for tData in tDatas:
            if float(tData['lastPeriod']) > 30:
                tData['consAbnormal'] = 2
            elif float(tData['lastPeriod']) < -30:
                tData['consAbnormal'] = 1

            if tData['lastPeriod'] != 0:
                datas.append(tData)

        self.modData('user_table_value', 'consAbnormal', datas)  

    # 执行数据处理
    def implement(self):
        self.tgData()
        # self.gateData()
        # self.TVData()
        # self.lineData() # 放在最后，因为要最后统计前面三者异常数

    def __del__(self):
        self.cur.close()
        self.coon.close()
        self.file.close()
