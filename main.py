import sys
import datetime

from spider import Spider
from process import Process


def printTips():
    print("input: main.exe; Spider newest data")
    print("input: main.exe xxxx-xx-xx; Spider From xxxx-xx-xx to the newest data")
    print("input: main.exe xxxx-xx-xx yyyy-yy-yy; Spider From xxxx-xx-xx to the yyyy-yy-yy data")
    exit()


if __name__ == "__main__":
    sign = 0
    try:
        if len(sys.argv) == 1:
            sign = 1
        elif len(sys.argv) == 2:
            begin = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
            end = datetime.datetime(3020, 1, 1)
        elif len(sys.argv) == 3:
            begin = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
            end = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')
        else:
            printTips()
    except Exception:
        printTips()

    if sign == 1:  # 爬取三天的，如果之前数据爬取有问题，三天之内会重爬
        for i in range(2, 5):
            date = datetime.datetime.now() - datetime.timedelta(days=i)
            s = Spider(date)
            s.implement()

            p = Process(date)
            p.implement()
    else:  # 爬取历史数据
        while begin <= (datetime.datetime.now() - datetime.timedelta(days=2)) and begin <= end:
            s = Spider(begin)
            s.implement()

            p = Process(begin)
            p.implement()

            begin += datetime.timedelta(days=1)
