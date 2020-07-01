import os
import time
import datetime
import pymysql

import settings as Set
from base import Base


class Process(Base):
    def __init__(self, date):
        Base.__init__(self, date)

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
                        COUNT(haveValue!=1 or null) AS abVal, \
                        COUNT(mpZxAbnormal!=0 or null) AS abZx, \
                        COUNT(mpFxAbnormal!=0 or null) AS abFx \
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

    # 清理当日爬取的无效数据
    def cleanInvalidData(self, tableName, fieldName, date):
        command = " DELETE FROM %s \
                    WHERE %s IS NULL \
                    AND statDate='%s';" % (tableName, fieldName, date)

        try:
            num = self.cur.execute(command)
            self.coon.commit()
            self.printResult(
                'date: %s; tableName: %s; clean invalid data %s row;' %
                (self.strDay, tableName, num), 'succ:  ')
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()

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
                (self.strDay, tableName, fieldName), 'succ:  ')
        except Exception as e:
            self.printResult(e, "error:  ")
            self.coon.rollback()

    # 台区电流,电压数据算法
    def electric(self, tgId, sign, datas):
        eDatas = []
        a = []
        b = []
        c = []
        loss = 0
        imbalance = 0

        for data in datas:
            if tgId == data['consTgId']:
                eDatas.append(data)

        if len(eDatas) < 3:
            return 0

        for eData in eDatas:
            for key, value in eData.items():
                if key == 'consTgId' or key == 'threePhase':
                    continue

                if value == '':
                    value = 0

                if eData['threePhase'] == 'ia' or eData['threePhase'] == 'ua':
                    a.append(float(value))
                elif eData['threePhase'] == 'ib' or eData['threePhase'] == 'ub':
                    b.append(float(value))
                elif eData['threePhase'] == 'ic' or eData['threePhase'] == 'uc':
                    c.append(float(value))

        for i in range(24):
            Min = min(a[i], b[i], c[i])
            Max = max(a[i], b[i], c[i])
            Count = a[i] + b[i] + c[i]

            if a[i] < 0.01 or b[i] < 0.01 or c[i] < 0.01:
                loss += 1

            if Count == 0:
                continue

            if (Max-Min)/Count > Set.THRESHOLD[sign]:
                imbalance += 1

        if loss > Set.THRESHOLD['DAY']:
            return 1
        elif imbalance > Set.THRESHOLD['DAY']:
            return 2
        else:
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

            if value < Set.THRESHOLD['FACTOR']:
                rate += 1

        if rate > Set.THRESHOLD['DAY']:
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

        if overload > Set.THRESHOLD['DAY']:
            return 2
        elif heavyload > Set.THRESHOLD['DAY']:
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
        aDatas = self.dateData(AIfield, 'line_abnormal_info', self.strDay)
        # 昨日数据
        yDatas = self.dateData(PIfield, 'line_power_info', self.strLastDay)
        # 今日数据
        tDatas = self.dateData(PIfield, 'line_power_info', self.strDay)
        # 异常台区数量数据
        tgDatas = self.abnormalDatas('tg', self.strDay)
        # 异常关口数量数据
        gateDatas = self.abnormalDatas('gate', self.strLastDay)
        # 异常专变数量数据
        hvDatas = self.abnormalDatas('hv', self.strDay)

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
        aDatas = self.dateData(AIfield, 'tg_abnormal_info', self.strDay)
        # 昨日数据
        yDatas = self.dateData(PIfield, 'tg_power_info', self.strLastDay)
        # 今日数据
        tDatas = self.dateData(PIfield, 'tg_power_info', self.strDay)
        # 台区容量数据
        bDatas = self.dateData(Bfield, 'tg_base')
        # 功率曲线算法
        pDatas = self.dateData(CVFfield, 'tg_power_curve', self.strDay)
        # 电流数据
        cDatas = self.dateData(CVFfield, 'tg_electric_current', self.strDay)
        # 电压数据
        vDatas = self.dateData(CVFfield, 'tg_electric_voltage', self.strDay)
        # 功率因数数据
        fDatas = self.dateData(CVFfield, 'tg_power_factor', self.strDay)

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
            
            if len(cDatas) == 0:
                tData['electricCur'] = 0
            else:
                tData['electricCur'] = self.electric(tData['tgId'], 'EC', cDatas)

            if len(vDatas) == 0:
                tData['electricVol'] = 0
            else:
                tData['electricVol'] = self.electric(tData['tgId'], 'EV', vDatas)

        self.modData('tg_power_info', 'lastPowerSup', tDatas)
        self.modData('tg_power_info', 'lastPowerSal', tDatas)
        self.modData('tg_power_info', 'abnormalSup', tDatas)
        self.modData('tg_power_info', 'abnormalSal', tDatas)
        self.modData('tg_power_info', 'abnormalType', tDatas)
        self.modData('tg_power_info', 'overload', tDatas)
        self.modData('tg_power_info', 'powerFactor', tDatas)
        self.modData('tg_power_info', 'electricCur', tDatas)
        self.modData('tg_power_info', 'electricVol', tDatas)

    # 关口数据分析
    def gateData(self):
        PIfield = ('id', 'mpId', 'zxPower', 'fxPower', 'zxSbd')
        # 昨日数据
        yDatas = self.dateData(PIfield, 'gateway_power_info', self.strLastDay)
        # 今日数据
        tDatas = self.dateData(PIfield, 'gateway_power_info', self.strDay)

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
        tDatas = self.dateData(PIfield, 'tg_table_value', self.strDay)

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
        userTable = 'user_table_value%s' % self.strTableMonth

        PIfield = ('id', 'consId', 'lastMrNum', 'thisRead', 'thisReadPq', 'lastReadPq', 'lastPeriod')
        # 今日数据
        tDatas = self.dateData(PIfield, userTable, self.strDay)

        datas = []
        for tData in tDatas:
            if tData['lastMrNum'] == '' or tData['thisRead'] == '':
                tData['consAbnormal'] = 1
            elif tData['thisReadPq'] == '0.00' or tData['lastReadPq'] == '0.00':
                tData['consAbnormal'] = 2
            elif float(tData['lastPeriod']) > 30:
                tData['consAbnormal'] = 3
            elif float(tData['lastPeriod']) < -30:
                tData['consAbnormal'] = 4
            else:
                tData['consAbnormal'] = 0

            if tData['consAbnormal'] != 0:
                datas.append(tData)
        print(len(datas))

        # self.modData(userTable, 'consAbnormal', datas)

    # 将北一区台区的 tg_power_info 数据和 tg_table_value 数据整合
    def tgMergeData(self):
        command = " SELECT COUNT(*) FROM tg_byq_info \
                    WHERE statDate='%s';" % (self.strDay)

        self.cur.execute(command)
        result = self.cur.fetchall()

        if result[0][0] > 0:
            self.printResult(
                'date: %s; tableName: %s; fieldName: %s; 当日数据已处理' %
                (self.strDay, 'tg_byq_info', 'byqField'), 'succ:  ')
            return

        byqField = (
            'orgId',  # '单位id',
            'orgName',  # '单位名称',

            'lineId',  # '线路id',
            'lineNo',  # '线路编号',
            'lineName',  # '线路名称',
            'lineType',

            'tgId',  # '台区id',
            'tgNo',  # '台区编号',
            'tgName',  # '台区名称',

            'abnormalType',  # '台区异常类型（0：正常，1：负损，2：高损，3：不可算）',

            'lastPowerSup',  # '上一日供电量',
            'powerSup',  # '供电量',
            'abnormalSup',  # '供电异常类型（0：正常，1：突减，2：突增）',

            'lastPowerSal',  # '上一日售电量',
            'powerSal',  # '售电量',
            'abnormalSal',  # '售电异常类型（0：正常，1：突减，2：突增）',

            'powerOut',  # '输出电量',
            'powerLoss',  # '损失电量',
            'rateLoss',  # '线损率',

            'accNum',  # '连续达标天数',
            'accNotNum',  # '连续不达标天数',
            'accStartDate',  # '连续开始日期',
            'ccNum',  # '达标情况',

            'tgMpNum',  # '台区总表数',
            'pmCopNum',  # '台区成功数',
            'pmSucEate',  # '台区采集成功率',

            'lvConsNum',  # '低压用户数',
            'vmCopNum',  # '用户成功数',
            'vmSucRate',  # '用户采集成功率',

            'overload',  # '重过载（0：正常，1：重载，2：过载）',
            'electricVol',  # '电压（0：正常，1：失压，2：三相不平衡）',
            'electricCur',  # '电流（0：正常，1：失流，2：三相不平衡）',
            'powerFactor',  # '功率因数（0：正常，1：异常）',

            'relAddMinusPap',  # '正向加减关系',
            'tFactor',  # '倍率',

            'mpLastZxPower',  # '昨日正向电量',
            'mpZxPower',  # '正向电量',
            'mpZxAbnormal',  # '正向电量异常（0：正常，1：突减，2：突增）',
            'mpLastFxPower',  # '昨日日反向电量',
            'mpFxPower',  # '反向电量',
            'mpFxAbnormal',  # '反向电量异常（0：正常，1：突减，2：突增）',

            'zxsbd',  # '正向上表底',
            'zxxbd',  # '正向下表底',
            'fxsbd',  # '反向上表底',
            'fxxbd',  # '反向下表底',

            'success',  # '是否成功',
            'haveValue',  # '有无表底（0：无，1：有）',

            'statDate'  # '日期',
        )

        # 电量信息字段
        field = (
            'pi.orgId',  # '单位id',
            'pi.orgName',  # '单位名称',

            'tv.PBLineId',  # '匹配的线路id',
            'tv.PBLineNo',  # '匹配的线路编号',
            'tv.PBLineName',  # '匹配的线路名称',
            'tv.PBLineType',  # '匹配的线路类型',

            'pi.tgId',  # '台区id',
            'pi.tgNo',  # '台区编号',
            'pi.tgName',  # '台区名称',

            'pi.abnormalType',  # '线损异常类型'

            'pi.lastPowerSup',  # 上一日供电量
            'pi.powerSup',  # '供电量',
            'pi.abnormalSup',  # '供电量异常类型'

            'pi.lastPowerSal',  # 上一日售电量
            'pi.powerSal',  # '售电量',
            'pi.abnormalSal',  # '售电量异常类型',

            'pi.powerOut',  # '输出电量',
            'pi.powerLoss',  # '损失电量',
            'pi.rateLoss',  # '线损率',

            'pi.accNum',  # '连续达标天数',
            'pi.accNotNum',  # '连续不达标天数',
            'pi.accStartDate',  # '连续开始日期',
            'pi.ccNum',  # '达标情况',

            'pi.tgMpNum',  # '台区总表数',
            'pi.pmCopNum',  # '台区成功数',
            'pi.pmSucEate',  # '台区采集成功率',

            'pi.lvConsNum',  # '低压用户数',
            'pi.vmCopNum',  # '用户成功数',
            'pi.vmSucRate',  # '用户采集成功率',

            'pi.overload',  # '重过载（0：正常，1：重载，2：过载）',
            'pi.electricVol',  # '电压（0：正常，1：失压，2：三相不平衡）',
            'pi.electricCur',  # '电流（0：正常，1：失流，2：三相不平衡）',
            'pi.powerFactor',  # '功率因数（0：正常，1：异常）',

            'tv.relAddMinusPap',  # '正向加减关系',
            'tv.tFactor',  # '倍率',

            'tv.mpLastZxPower',  # '昨日正向电量',
            'tv.mpZxPower',  # '正向电量',
            'tv.mpZxAbnormal',  # '正向电量异常（0：正常，1：突减，2：突增）',
            'tv.mpLastFxPower',  # '昨日日反向电量',
            'tv.mpFxPower',  # '反向电量',
            'tv.mpFxAbnormal',  # '反向电量异常（0：正常，1：突减，2：突增）',

            'tv.zxsbd',  # '正向上表底',
            'tv.zxxbd',  # '正向下表底',
            'tv.fxsbd',  # '反向上表底',
            'tv.fxxbd',  # '反向下表底',

            'tv.success',  # '是否成功',
            'tv.haveValue',  # '有无表底（0：无，1：有）',

            'pi.statDate',  # '日期',
        )

        command = " CREATE TEMPORARY TABLE tmp_power_info \
                    SELECT * FROM tg_power_info \
                    WHERE tgId IN (SELECT tgId FROM tg_byq) \
                    AND statDate='%s' \
                    GROUP BY tgId;" % (self.strDay)

        self.cur.execute(command)

        command = " CREATE TEMPORARY TABLE tmp_table_value \
                    SELECT * FROM tg_table_value \
                    WHERE consTgId IN (SELECT tgId FROM tg_byq) \
                    AND statDate='%s' \
                    GROUP BY consTgId;" % (self.strDay)

        self.cur.execute(command)

        strByqField = self.operationField(byqField)
        strField = self.operationField(field)

        command = " INSERT INTO tg_byq_info(%s) \
                    SELECT %s FROM tmp_power_info AS pi \
                    LEFT JOIN tmp_table_value AS tv \
                    ON pi.tgId=tv.consTgId;" % (strByqField, strField)

        self.cur.execute(command)

        self.printResult(
                'date: %s; tableName: %s; fieldName: %s;' %
                (self.strDay, 'tg_byq_info', 'byqField'), 'succ:  ')

        command = "DROP TABLE tmp_power_info"
        self.cur.execute(command)

        command = "DROP TABLE tmp_table_value"
        self.cur.execute(command)

    # 执行数据处理
    def implement(self):
        # 数据清洗
        self.cleanInvalidData('tg_power_info', 'powerSup', self.strDay)

        # 数据分析处理
        self.tgData()
        self.gateData()
        self.TVData()
        # self.UTVData()
        self.lineData()  # 放在这里，因为要最后统计前面三者异常数
        self.tgMergeData()  # 放在这里，因为要结合 tg_power_info 和 tg_table_value

    def __del__(self):
        Base.__del__(self)
