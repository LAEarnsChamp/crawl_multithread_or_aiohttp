# -*- encoding: utf-8 -*-
'''
@Description: some description
@Date       : 2021/08/11
@Author     : linbin
@version    : v1.0
'''

import json
import time
import concurrent

import requests
import pandas as pd
from pyquery import PyQuery as pq
from sqlalchemy.engine import create_engine


class HousePriceException(Exception):
    """方便区分问题"""
    pass


def default() -> dict:
    """默认格式
    """
    return {
        'address': "",
        'lng': 0,
        'lat': 0,
        'precise': 0,
        'confidence': 0,
        'comprehension': 0,
        'level': "无",
        "price": "0"
    }


def get_headers() -> str:
    """获取请求头"""
    return {
        'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 YaBrowser/19.7.0.1635 Yowser/2.5 Safari/537.36'
    }


def retry(retry_times):
    """重试"""
    retry_times = retry_times
    if retry_times is None:
        retry_times = 5

    def wrapper(func):
        def _wrapper(*args, **kwargs):
            retry_ = retry_times
            while retry_ > 0:
                try:
                    retry_ -= 1
                    return func(*args, **kwargs)
                except HousePriceException as e1:  # 请求问题打印信息自动重试
                    time.sleep(1)
                    print(f'[Request Aborted] {e1}\nbegin retrying ...')

            print(f"[Retried Failed], info: {kwargs}")
            return None  # 重试失败返回None

        return _wrapper

    return wrapper


@retry(retry_times=5)
def get_address(address) -> tuple:
    """请求地址信息"""
    try:
        address = address.replace("#", "&")  # type: str
    except Exception:
        print('repalce_error:%s' % (address))

    # 请求地址信息
    baidu_api = requests.get(
        f'http://api.map.baidu.com/geocoding/v3/?address={address}&output=json&ak=So9Mbym2UKR9DR9VRNp0m9TxQ1l5sOk1'
    )
    baidu_location = json.loads(baidu_api.text)  # type: dict
    if baidu_location.get("status") != 0:
        raise HousePriceException(  # 抛给重试函数处理
            f'请求地址信息失败 code: {baidu_location.get("status")}')

    address_result = baidu_location.get("result")  # type: dict

    address = address
    lng = address_result.get("location").get("lng")
    lat = address_result.get("location").get("lat")
    precise = address_result.get("precise")
    confidence = address_result.get("confidence")
    comprehension = address_result.get("comprehension")
    level = address_result.get("level")

    return address, lng, lat, precise, confidence, comprehension, level


@retry(retry_times=5)
def get_price(address) -> str:
    """get house price from address"""
    time.sleep(1)  # 毕竟不是API服务

    # 请求附近房价
    address = address.replace(" ", "")
    se = requests.session()
    se.headers.update(get_headers())
    price_api = se.get(
        f'https://m.creprice.cn/ha/indexSearch.html?keyword={address}')
    if price_api.status_code != 200:  # 非api可能有反爬，抛给重试函数
        raise HousePriceException(f'请求房源信息失败 code: {price_api.status_code}')

    price_doc = pq(price_api.text)
    price_detail_url = price_doc('.searchlist a').attr('href')
    price_detail = price_doc('.cont_01 .data .fl span').html()
    if price_detail_url is not None:
        # 如果是房源选择界面，则需要请求第一个选项的
        price_api2 = se.get(price_detail_url)
        if price_api2.status_code != 200:  # 非api可能有反爬，抛给重试函数
            raise HousePriceException(
                f'请求房源详细信息页面失败 code: {price_api2.status_code}')
        price_doc2 = pq(price_api2.text)
        price_detail = price_doc2(
            '.cont_01 .data .fl span').html()  # type: str

    if price_detail is None:  # 请求顺利但数据没有考虑是页面的问题
        print(f"请求房价页面成功，但无法获取数据，将返回0，请检查返回页面，地址：{address}")
        return 0

    try:
        return int(price_detail.replace(",", ""))
    except ValueError:  # 有返回，当值为'--'之类的
        return price_detail


def run(address: str) -> json:
    """隔离两个请求函数
    当某一个不成功时还能返回另一个的结果
    其中不成功的重试后依然失败将返回None，最终信息为0
    """
    result = default()  # 默认格式

    if get_address(address) is not None:  # 如果地址信息请求成功
        key_list = [
            'address', 'lng', 'lat', 'precise', 'confidence', 'comprehension',
            'level'
        ]
        value_list = get_address(address)
        for k, v in zip(key_list, value_list):
            result[k] = v
    if get_price(address) is not None:  # 如果房价信息请求成功
        result["price"] = get_price(address)
    return pd.DataFrame(result, index=[0])


if __name__ == '__main__':
    """多线程执行"""
    engine = create_engine(
        'presto://analysis@presto-gateway-rancher-prodhd.inner.youdao.com:80/hive'
    )

    sql = """ --prefer(etl_market)
        select * from temp.panzhj_baidu_request_today
        """
    address_basic = pd.read_sql(sql, engine)
    address_pool = [
        address_basic.at[i, 'address'] for i in range(len(address_basic))
    ]

    results = []
    print("running...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(run, address) for address in address_pool]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    df_result = pd.concat(results, axis=0)
    df_result.sort_index(inplace=True)
    df_result.to_csv("result.csv", encoding='utf-8', index=None)
