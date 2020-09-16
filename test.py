import sys
import datetime
import configparser
import os


def printTips():
    print('------------------------EicData------------------------')
    print('在 mod 中输入以下模式（n 和 h 必须二选一）：')
    print()
    print('    n : newset 最新数据              ')
    print('    h : histroy 历史数据              ')
    print()
    print('    s : spider 爬取数据               ')
    print('    p : process 处理数据              ')
    print()
    print('例如：nsp 爬取最新数据并处理，hsp 爬取历史数据并处理')
    print('-------------------------------------------------------')


def printHelps():
    print("input: main.exe; Spider newest data")
    print("input: main.exe xxxx-xx-xx; Spider From xxxx-xx-xx to the newest data")
    print("input: main.exe xxxx-xx-xx yyyy-yy-yy; Spider From xxxx-xx-xx to the yyyy-yy-yy data")


def operation(mod, date):
    if 's' in mod:
        print('spider' + date.strftime('%Y-%m-%d'))

    if 'p' in mod:
        print('process' + date.strftime('%Y-%m-%d'))


def newest(mod):
    for i in range(2, 5):
        date = datetime.datetime.now() - datetime.timedelta(days=i)

        operation(mod, date)


def histroy(mod, begin, end):
    while begin <= (datetime.datetime.now() - datetime.timedelta(days=2)) and begin <= end:
        operation(mod, begin)

        begin += datetime.timedelta(days=1)


def directRun():
    printTips()
    mod = input("input mod:")

    if 'n' in mod and 'h' in mod:
        print('error')
        return

    if 'n' in mod:
        newest(mod)

    if 'h' in mod:
        try:
            begin = datetime.datetime.strptime(input("input begin date(yyyy-mm-dd):"), '%Y-%m-%d')
            end = datetime.datetime.strptime(input("input end date(yyyy-mm-dd):"), '%Y-%m-%d')
            histroy(mod, begin, end)
        except Exception:
            print('error')


def parameterRun(parameter):
    if len(parameter) < 2:
        print('error')
        return

    mod = parameter[1]

    if '--help' in mod:
        printHelps()
        return

    if not ('-' in mod and mod.count('-') == 1):
        print('error')
        return

    if 'n' in mod:
        newest(mod)

    if 'h' in mod:
        try:
            if len(parameter) == 3:
                begin = datetime.datetime.strptime(parameter[2], '%Y-%m-%d')
                end = datetime.datetime(3020, 1, 1)
            elif len(parameter) == 4:
                begin = datetime.datetime.strptime(parameter[2], '%Y-%m-%d')
                end = datetime.datetime.strptime(parameter[3], '%Y-%m-%d')
            else:
                printHelps()
                return
            histroy(mod, begin, end)
        except Exception:
            printHelps()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        directRun()
    else:
        parameterRun(sys.argv)

    os.system('pause')
