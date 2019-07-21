# coding:utf-8
"""
从各个代理IP网站抓取免费代理IP
1、云代理 www.ip3366.net
2、旗云代理 http://www.qydaili.com/
3、unknown http://www.goubanjia.com
4、快代理 http://www.kuaidaili.com/free/inha/
5、89免费代理 http://www.89ip.cn/index_1.html
6、IP海代理 http://www.iphai.com/free/ng
7、极速代理 http://www.superfastip.com/welcome/freeip/1
8、西刺代理  https://www.xicidaili.com/nn/

TODO 代理抓取通用爬虫
"""
import time

import requests
import pandas as pd
from faker import Faker

from redis_helper import RedisHelper


class CatchProxyIp:
    def __init__(self):
        self.headers = {
            'User-Agent': Faker().user_agent()
        }
        self.db = RedisHelper()
        self.save_to_redis = self.db.save_proxy_ip

    def merge_col(self, frame, func=None, symbol=''):
        """
        主要是功能是将多列按某种方式拼接在一起，然后在进行func操作
        比如将某三列A、B、C在中间加下划线方式拼接，然后转MD5,则调用方式如下
        merge_col(frame=df[['A','B','C']], func=MD5_encode, symbol='_')
        :return: Series format

        """
        row, col = frame.shape
        if col < 2:
            raise Exception(u'DataFrame至少两列以上才能拼接')

        ret = frame.apply(lambda x: symbol.join(map(str, x)), axis=1)
        return ret.apply(func) if func else ret

    # 云代理 www.ip3366.net  24小时更新一次 （100条）
    def cloud_proxy(self):
        try:
            header = self.headers.copy()
            header['Host'] = 'www.ip3366.net'
            # 仅有7页数据
            urls = ['http://www.ip3366.net/free/?stype=1&page={page}'.format(page=str(i)) for i in range(1, 8)]
            frames = []
            for link in urls:
                # time.sleep(1)
                ret = requests.get(link, headers=header)
                ret.encoding = 'gb2312'
                df = pd.read_html(io=ret.content, header=0)[0]
                frames.append(df)
            df = pd.concat(frames)
            ip_lst = self.merge_col(df[['IP', 'PORT']], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('cloud_proxy - ', rate)
        except Exception as e:
            print(e)

    # http://www.qydaili.com/free/?action=china&page=1 旗云代理 每小时小量更新
    def qydaili_proxy(self):
        try:
            header = self.headers.copy()
            # 仅有10页数据
            urls = ['http://www.qydaili.com/free/?action=china&page={page}'.format(page=str(i)) for i in range(1, 11)]
            frames = []
            for link in urls:
                # time.sleep(1)
                ret = requests.get(link, headers=header)
                # ret.encoding = 'gb2312'
                df = pd.read_html(io=ret.content, header=0)[0]
                frames.append(df)
            df = pd.concat(frames)
            ip_lst = self.merge_col(df[['IP', 'PORT']], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('qydaili_proxy - ', rate)
        except Exception as e:
            print(e)

    # http://www.goubanjia.com  10条高质量免费代理）
    def goubanjia_proxy_free(self):
        try:
            header = self.headers.copy()
            header['Host'] = 'www.goubanjia.com'
            link = 'http://www.goubanjia.com/'
            ret = requests.get(link, headers=header)
            ret.encoding = 'gb2312'
            df = pd.read_html(io=ret.text, header=0)[0]
            ip_lst = [i.replace('..', '.') for i in df['IP:PORT'].tolist()]
            rate = self.save_to_redis(ip_lst)
            print('goubanjia_proxy_free - ', rate)
        except Exception as e:
            print(e)

    # http://www.kuaidaili.com/free/inha/  快代理 几百页免费代理 （20000+）
    def kuai_proxy(self):
        try:
            header = self.headers.copy()
            header['Host'] = 'www.kuaidaili.com'
            urls = ['http://www.kuaidaili.com/free/inha/{page}/'.format(page=str(i)) for i in range(1, 5)]
            frames = []
            for link in urls:
                time.sleep(1)
                ret = requests.get(link, headers=header)
                # ret.encoding = 'gb2312'
                df = pd.read_html(io=ret.content, header=0)[0]
                frames.append(df)
            df = pd.concat(frames)
            ip_lst = self.merge_col(df[['IP', 'PORT']], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('kuai_proxy - ', rate)
        except Exception as e:
            print(e)

    # http://www.89ip.cn/index_1.html 89代理 10多页
    def ip89_proxy(self):
        try:
            header = self.headers.copy()
            # 34页
            urls = ['http://www.89ip.cn/index_{page}.html'.format(page=str(i)) for i in range(1, 11)]
            frames = []
            for link in urls:
                ret = requests.get(link, headers=header)
                df = pd.read_html(io=ret.content, header=0)[0]
                frames.append(df)
            df = pd.concat(frames)
            ip_lst = self.merge_col(df.iloc[:, :2], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('ip89_proxy - ', rate)
        except Exception as e:
            print(e)

    # http://www.iphai.com/free/ng IP海代理 一页
    def iphai_proxy(self):
        try:
            header = self.headers.copy()
            # 34页
            link = 'http://www.iphai.com/free/ng'
            ret = requests.get(link, headers=header)
            print(ret.status_code)
            df = pd.read_html(io=ret.content, header=0)[0]
            ip_lst = self.merge_col(df.iloc[:, :2], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('iphai_proxy - ', rate)
        except Exception as e:
            print(e)

    # http://www.superfastip.com/welcome/freeip/1 极速代理 10多页
    def superfastip_proxy(self):
        try:
            header = self.headers.copy()
            # 34页
            urls = ['http://www.superfastip.com/welcome/freeip/{page}'.format(page=str(i)) for i in range(1, 11)]
            frames = []
            for link in urls:
                ret = requests.get(link, headers=header)
                df = pd.read_html(io=ret.content, header=0)[1]
                frames.append(df)
            df = pd.concat(frames)
            ip_lst = self.merge_col(df.iloc[:, :2], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('superfastip_proxy - ', rate)
        except Exception as e:
            print(e)

    # https://www.xicidaili.com/nn/ 西刺代理
    def xici_proxy(self):
        try:
            header = self.headers.copy()
            # 34页
            urls = ['https://www.xicidaili.com/nn/{page}'.format(page=str(i)) for i in range(1, 4)]
            frames = []
            for link in urls:
                ret = requests.get(link, headers=header)
                df = pd.read_html(io=ret.content, header=0)[0]
                frames.append(df)
            df = pd.concat(frames)
            ip_lst = self.merge_col(df.iloc[:, 1:3], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('xici_proxy - ', rate)
        except Exception as e:
            print(e)

    # https://proxy.mimvp.com/freesecret.php?proxy=in_hp&sort=&page=1 mimvp代理
    def mimvp_proxy(self):
        try:
            header = self.headers.copy()
            # 34页
            urls = ['https://proxy.mimvp.com/freesecret.php?proxy=in_hp&sort=&page={page}'.format(page=str(i)) for i in range(1, 11)]
            frames = []
            for link in urls:
                ret = requests.get(link, headers=header)
                print(ret.text)
                df = pd.read_html(io=ret.content, header=0)[0]
                print(df)
                frames.append(df)
            df = pd.concat(frames)
            ip_lst = self.merge_col(df.iloc[:, 1:3], None, ':').tolist()
            rate = self.save_to_redis(ip_lst)
            print('mimvp_proxy - ', rate)
        except Exception as e:
            print(e)

    def catch(self):
        self.cloud_proxy()
        self.kuai_proxy()
        self.goubanjia_proxy_free()
        self.qydaili_proxy()
        self.ip89_proxy()
        self.iphai_proxy()
        self.superfastip_proxy()
        self.xici_proxy()
        # self.mimvp_proxy()


if __name__ == '__main__':
    # TODO 合并代码  代码重复率太高
    CatchProxyIp().catch()
