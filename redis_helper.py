from redis import StrictRedis
from multiprocessing.dummy import Pool

from config import BaseConfig
from ip_check import is_valid_proxy


class RedisHelper():

    def __init__(self):
        self.ip_key = 'proxy_ip'
        self.movie_key = 'movie_id'
        self.con = StrictRedis.from_url(BaseConfig.redis_cfg, decode_responses=True)

    def save_proxy_ip(self, ips: list):
        """
        保存代理ip
        :param ips: list
        :return: None
        """
        p = Pool(20)
        ret = p.map(is_valid_proxy, ips)
        proxy_ips = [i for i in ret if i]
        if proxy_ips:
            self.con.lpush(self.ip_key, *proxy_ips)
        return f'Effective rate: {len(proxy_ips)}/{len(ips)}'

    def get_proxy_ip(self):
        """
        单个代理ip获取
        出队后会进行校验，无效的被剔除，有效的反向入队便于下次使用
        :return: ip:port
        """
        while True:
            proxy_ip = self.con.brpop(self.ip_key)[1]
            print(proxy_ip, 'POP')
            if is_valid_proxy(proxy_ip):
                self.con.lpush(self.ip_key, proxy_ip)
                return proxy_ip

    def save_movie_id(self, ids):
        """
        保存豆瓣电影id
        :param code: 豆瓣电影id 或 豆瓣电影id列表
        :return: 豆瓣电影id数量
        """
        if isinstance(ids, str):
            ids = [ids]

        self.con.lpush(self.movie_key, *ids)

        return self.con.llen(self.movie_key)

    def get_movie_id(self):
        """
        movie id
        :return: 豆瓣电影id 和 Redis中豆瓣电影id数量
        """
        code = self.con.brpop(self.movie_key)[1]
        return code, self.con.llen(self.movie_key)

    def clear_movie_id(self):
        self.con.delete(self.movie_key)


if __name__ == '__main__':
    db = RedisHelper()
    proxy_ip = db.get_proxy_ip()
    print(proxy_ip)
