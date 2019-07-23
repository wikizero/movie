import re
import time
import json
import urllib3
from pprint import pprint
from urllib import parse
from multiprocessing.dummy import Pool

import requests
from faker import Faker
from bs4 import BeautifulSoup
from pymongo import MongoClient

from config import BaseConfig
from redis_helper import RedisHelper

urllib3.disable_warnings()


class CatchMovieInfo():
    def __init__(self):
        # 线程数量
        self.pro_num = 20

        # 记录失败的code的处理次数，达到N次将不会再次进队
        self.fail_id = {}

        # 连接MongoDB
        self.conn = MongoClient(BaseConfig.mongo_cfg)
        self.db = self.conn.movies

        # 获取RedisHelper
        self.redis_helper = RedisHelper()

    def send_request(self, url):
        try:
            headers = {'User-Agent': Faker().user_agent()}

            proxy_ip = self.redis_helper.get_proxy_ip()
            proxies = {
                'http': 'http://' + proxy_ip,
                'https': 'https://' + proxy_ip
            }
            res = requests.get(url, headers=headers, proxies=proxies, timeout=5, verify=False)
            status_code = res.status_code
            if status_code < 200 or status_code > 300:
                print(url, 'Request error')
                raise Exception('Request error')
            return res

        except Exception as e:
            # print_exc()
            pass

    def catch_movie_info(self, movie_id):
        try:
            data = {}
            data['_id'] = movie_id
            url = 'https://movie.douban.com/subject/{movie_id}/'.format(movie_id=movie_id)
            data['url'] = url

            res = self.send_request(url)
            if not res:
                raise Exception('send_request error')

            obj = BeautifulSoup(res.content, 'lxml').select('#content')[0]

            data['watch'] = [
                {'source': i.a.string.strip(), 'url': i.a['href'].strip(), 'charge': i.span.span.string.strip()}
                for i in obj.select('.gray_ad li')]

            data['name'] = obj.select('h1 span')[0].string
            data['year'] = re.findall(r'\d+', obj.select('h1 span')[1].string)[0]

            data['actor'] = [item.string for item in obj.select('.actor a')]

            data['type'] = [item.string for item in obj.select("[property='v:genre']")]

            duration = obj.select("[property='v:runtime']")
            data['duration'] = duration[0].string if duration else ''  # 2018-5-24 部分影片无片长

            data['votes'] = obj.select("[property='v:votes']")[0].string

            data['score'] = obj.select("[property='v:average']")[0].string

            data['releaseDate'] = [item['content'] for item in obj.select("[property='v:initialReleaseDate']")]

            desc = obj.select("[property='v:summary']")
            if desc:
                data['desc'] = '\n'.join([i.strip() for i in desc[0].get_text().split('\n') if i.strip()])  # desc
            else:
                data['desc'] = ''  # 2018-5-24 部分影片无描述信息

            data['img_url'] = obj.select('#mainpic img')[0]['src']  # img_url

            data['douban_recommend'] = list(
                set(link['href'].split('/')[4] for link in obj.select('.recommendations-bd a')))

            # 存在则更新，不存在则插入
            self.db.raw.update({'_id': data['_id']}, data, upsert=True)
            # pprint(data)
            # TODO IMDB、 国家地区、语言、alias、
            # TODO IMDB 评分  推荐

        except Exception as e:
            # 异常 返回movie id
            # traceback.print_exc()
            self.fail_id.setdefault(movie_id, 0)
            self.fail_id[movie_id] += 1
            return movie_id

    def extract_movie_id(self, url):
        try:
            res = self.send_request(url)
            if not res:
                print(url, 'continue')
            data = json.loads(res.text)
            movie_ids = [i['id'] for i in data['data']]
            cur_sum = self.redis_helper.save_movie_id(movie_ids)
            print(url, 'success')
            print('movie_id:', cur_sum)
        except Exception as e:
            pass

    def worker(self, pid):
        """
        从redis获movie id，去采movie info
        :return:
        """
        while True:
            _id, _len = self.redis_helper.get_movie_id()
            print(f'[worker_id:{pid}]剩余movie id数量:{_len}')

            # 判断movie id失败次数是否超过3次，如果失败次数超过3次则不再处理
            if self.fail_id.get(_id, 0) > 3:
                self.fail_id[_id] = 0  # 清零
                continue

            movie_id = self.catch_movie_info(_id)

            if movie_id:
                self.redis_helper.save_movie_id(movie_id)

    def catch_movie_id(self):
        """
        catch movie id and push to redis queue
        :return:
        """
        # https://movie.douban.com/tag/#/?sort=U&range=9,10&tags=电影
        movie_count = 20000
        data = {
            'sort': 'U',
            'range': '0,10',
            'tags': '电影',
            'limit': 200
        }
        data['tags'] = parse.quote(data['tags'])
        url = 'https://movie.douban.com/j/new_search_subjects?sort={sort}&range={range}&tags={tags}&limit={limit}'
        url = url.format(**data)
        url += '&start={start}'
        urls = [url.format(start=i) for i in range(0, movie_count, data.get('limit', 20))]
        p = Pool(10)
        p.map(self.extract_movie_id, urls)

    def start_worker(self):
        """
        启动worker，开始采集电影信息
        :return:
        """
        p = Pool((self.pro_num))
        p.map(self.worker, list(range(1, self.pro_num + 1)))


if __name__ == '__main__':
    # TODO 更新数据
    # TODO 当前只采集了tag=电影   还有电视剧、综艺、动漫、记录片、短片
    # TODO 《升级》

    # TODO 代理IP可用率太低
    # TODO 数据不完整

    # TODO 方向
    # 1、把电影数据库搭建起来
    # 2、数据每天刷新
    # 3、资源整合，关联到电影天堂、小调网、ck电影、bt天堂等网站、通过百度搜索关联（获取下载链接）
    # 4、部分资源迅雷无法下载，提供下载器（断点续传、多线程、种子解析等功能）

    # TODO 资源来源
    # https://www.piaohua.com/
    # https://www.xiaopian.com/
    # http://www.hao6v.com/
    # https://www.xl720.com/
    # https://www.dygod.net/
    # http://www.2btbtt.com/
    # http://www.longbaidu.com/
    # http://www.kk2w.cc/

    # TODO CHD网络爬虫
    # 下载速度 排序？
    # 5、推荐系统

    # movie_info(26811587)
    # CatchMovieInfo().catch_movie_id()
    CatchMovieInfo().start_worker()
