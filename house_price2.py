# -*- encoding: utf-8 -*-
'''
@Description: 无差别爬取所有镇/街道
@Date       : 2021/08/18
@version    : v2.0
'''
"""
首先爬取首页，正则匹配所有的二级页面的地址，分发给多个二级页面爬取+解析器，由aiohttp执行爬取
"""

import re
import requests
import aiohttp
import asyncio

import pandas as pd


def get_headers() -> str:
    """获取请求头"""
    return {
        'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 YaBrowser/19.7.0.1635 Yowser/2.5 Safari/537.36'
    }


def first_spider(url: str) -> list:
    """首页爬取并解析网址"""
    headers = get_headers()
    response = requests.get(url, headers=headers)
    html_doc = response.text
    return re.findall(
        "re",
        html_doc)


async def second_spider(url_city: tuple, session: aiohttp.ClientSession) -> list:
    """爬取二级页面并解析房价"""
    url, city = url_city
    url = "url" + url  # 拼接前缀
    cnt = 5
    while cnt > 0:
        try:
            await asyncio.sleep(1)
            response = await session.get(url)
            html_doc = await response.text(encoding='utf-8')
            return [city] + re.findall(
                "re",
                html_doc,
            )

        except Exception as e:
            print(f'[error] {e}, url: {url}, retrying...')
            cnt += 1
            continue
    print("Retry finished. Failed to get data")


async def load_tasks(urls_cities: list) -> pd.DataFrame:
    """注册任务"""
    # 全局对象
    global conn
    global session
    conn = aiohttp.TCPConnector(limit=5)  # 控制连接量
    session = aiohttp.ClientSession(connector=conn,
                                    timeout=aiohttp.ClientTimeout(total=8000))
    list_tasks = [
        asyncio.ensure_future(second_spider(url_city, session))
        for url_city in urls_cities
    ]
    tasks = await asyncio.gather(*list_tasks)

    # 先用dict装填list再生成df
    list_ = []
    for task in tasks:
        city = task[0]
        for area_price in task[1:]:
            list_.append({
                '城市': city,
                '区域': area_price[0],
                '均价': area_price[1]
            })
    return pd.DataFrame(list_)


if __name__ == "__main__":
    firt_url = 'url'
    second_urls_cities = first_spider(firt_url)

    loop = asyncio.get_event_loop()
    df_result = loop.run_until_complete(load_tasks(second_urls_cities))
    loop.run_until_complete(conn.close())
    loop.run_until_complete(session.close())
    # 在关闭loop之前要给aiohttp一点时间关闭ClientSession
    loop.run_until_complete(asyncio.sleep(3))

    df_result = df_result.drop_duplicates()
    df_result.to_csv('result.csv', encoding='utf-8-sig', index=None)
