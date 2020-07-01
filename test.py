import sys
import datetime
import configparser


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

    if sign == 1:
        for i in range(2, 5):
            date = datetime.datetime.now() - datetime.timedelta(days=i)
            print(date)
    else:
        while begin <= (datetime.datetime.now() - datetime.timedelta(days=2)) and begin <= end:
            print(begin)
            begin += datetime.timedelta(days=1)
        print(end)

    # 读取配置文件
    config = configparser.ConfigParser()
    config.read('conf.ini')
    db = {}
    for item in config.items('db'):
        db[item[0]] = item[1]

    print(db)
