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
9、西拉免费代理IP  http://www.xiladaili.com/https/1/  可用率比较高有反爬限制
10、http://www.nimadaili.com/gaoni/  可用率比较高有反爬限制
11、http://ip.kxdaili.com/ipList/1.html#ip
12、http://31f.cn/
13、http://www.shenjidaili.com/shareip/    http代理(处理方式不一致, 未处理)
14、http://www.66ip.cn/areaindex_19/1.html   有反爬限制，js动态加载
15、http://feilongip.com/
16、http://www.dlnyys.com/free/
"""
import re
import time
import random
from multiprocessing.dummy import Pool

import requests
from faker import Faker

from redis_helper import RedisHelper


class CatchProxyIp:
    def __init__(self):
        self.headers = {
            'User-Agent': Faker().user_agent()
        }
        self.db = RedisHelper()
        self.save_to_redis = self.db.save_proxy_ip

        self.pattern_tags = re.compile(r'<[^>]+>', re.S)
        self.pattern_blank = re.compile(r'\s+', re.S)
        self.pattern_colon = re.compile(r' ', re.S)
        self.pattern_ip = re.compile(r'(?:\d+\.){3}\d+:\d+')

        # 搜索深度(页数)
        self.search_depth = 5

    def get_urls(self):
        urls = []
        origin_urls = [
            'http://www.nimadaili.com/gaoni/{page}/',
            'http://www.nimadaili.com/http/{page}/',
            'http://www.nimadaili.com/https/{page}/',
            'http://www.xiladaili.com/https/{page}/',
            'http://www.xiladaili.com/putong/{page}/',
            'http://www.xiladaili.com/gaoni/{page}/',
            'http://www.xiladaili.com/https/{page}/',
            'https://www.xicidaili.com/nn/{page}',
            'http://www.superfastip.com/welcome/freeip/{page}',
            'http://www.89ip.cn/index_{page}.html',
            'http://www.kuaidaili.com/free/inha/{page}/',
            'http://www.qydaili.com/free/?action=china&page={page}',
            'http://www.ip3366.net/free/?stype=1&page={page}',
            'http://ip.kxdaili.com/ipList/{page}.html#ip',
            'http://www.dlnyys.com/free/inha/{page}/',

        ]
        for url in origin_urls:
            urls += [url.format(page=str(i)) for i in range(1, self.search_depth)]
        urls.append('http://31f.cn/')
        urls.append('http://feilongip.com/')
        return urls

    def send_request(self, url):
        try:
            ret = requests.get(url, headers=self.headers)
            ip_lst = self.extract_proxy_ip(ret.text)
            if ip_lst:
                rate = self.save_to_redis(ip_lst)
                print(f'{url} - rate:{rate}')
            else:
                print(f'{url} - None')
        except Exception as e:
            print(e)

    def extract_proxy_ip(self, html):
        # 删除所有html标签
        text = self.pattern_tags.sub('', html)
        # 将空白符替换成空格
        text = self.pattern_blank.sub(' ', text)
        # print(text)
        # 两数字之前的空格替换成冒号
        text = self.pattern_colon.sub(':', text)
        # print(text)
        # 提取代理ip
        proxy_ip_lst = self.pattern_ip.findall(text)

        return proxy_ip_lst

    def catch(self):
        p = Pool(3)
        urls = self.get_urls()
        random.shuffle(urls)
        p.map(self.send_request, urls)


if __name__ == '__main__':
    CatchProxyIp().catch()

