import requests


def is_valid_proxy(ip, url='https://movie.douban.com/', timeout=5):
    """
    校验代理ip是否能访问指定url（代理ip是否有效）
    :param ip: 127.0.0.1:8888
    :param url: https://www.baidu.com/
    :param timeout: 超时时间
    """
    proxies = {
        'http': 'http://' + ip,
        'https': 'https://' + ip
    }

    try:
        ret = requests.get(url, proxies=proxies, timeout=timeout)
    except Exception as e:
        print(ip, 'FALSE')
        return False

    if 200 <= ret.status_code < 300:
        print(ip, 'TRUE')
        return ip

    print(ip, 'FALSE')
