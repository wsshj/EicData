import datetime

import spider
import process

if __name__ == "__main__":
    # for i in range(2, 9):    # 爬取七天的，如果之前数据爬取有问题，七天之内会重爬
    #     date = datetime.datetime.now() - datetime.timedelta(days=i)
    #     spider = spider.Spider(date)
    #     spider.implement()

    #     process = process.Process(date)
    #     process.implement()

    ssdate = datetime.date(2020, 4, 19)
    for i in range(35):
        date = ssdate - datetime.timedelta(days=i)
        p = process.Process(date)
        p.implement()

    # date = datetime.date(2020, 4, 19)

    # p = process.Process(date)
    # p.implement()
