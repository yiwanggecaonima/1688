# coding=utf-8
import asyncio
import json
import re
import aiohttp
import aiomysql
from lxml import etree
import redis
from logger import crawler

class A_1688_Detail():
    def __init__(self):
        self.MAX_C = 100
        self.semaphore = asyncio.Semaphore(self.MAX_C)  # 控制并发数
        crawler.warning("控制并发数 {}".format(str(self.MAX_C)))
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
                        'Cookie': ''}
        self.redis_client = redis.Redis(host="localhost", port=6379, db=2)


    async def fetch(self, url, session):
        try:
            async with session.get(url, headers=self.headers, timeout=60, verify_ssl=False) as resp:
                if resp.status in [200, 201]:
                    data = await resp.text()
                    return data
        except Exception as e:
            print(e)
            pass

    def doc_xpath(self,html):
        return etree.HTML(html)

    def extract_elements(self, source):
        doc = self.doc_xpath(source)
        item = {}
        title = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='contact-info']/h4/text()")
        item["title"] = title[0] if len(title) > 0 else "标题未知"
        name = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='contact-info']/dl/dd/a[@class='membername']/text()")
        item["name"] = name[0] if len(name) > 0 else "联系人未知"
        address = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='fd-line']/div[@class='contcat-desc']//dd[@class='address']/text()")
        item["address"] = address[0].replace('\n','').replace(' ','') if len(address) > 0 else "地址未知"

        web = doc.xpath("//div[@class='contcat-desc']/dl/dd/div/a[@class='subdomain']/text()")
        item["web"] = web[0] if len(web) > 0 else "公司主页 未知"

        tel1 = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='fd-line']/div[@class='contcat-desc']/dl[1]/dd/text()")
        item["tel1"] = tel1[0].replace('\n', '') if len(tel1) > 0 else "电话未知"

        tel2 = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='fd-line']/div[@class='contcat-desc']/dl[2]/dd/text()")
        tel2 = tel2[0].replace('\n', '').replace(' ','') if len(tel2) > 0 else None
        if tel2:
            item["tel2"] = tel2
        else:
            try_tel2 = doc.xpath("//div[@class='m-content']/dl[@class='m-mobilephone']/dd/text()")
            item["tel2"] = try_tel2[0].replace('\n', '').replace(' ','') if len(try_tel2) > 0 else "移动电话未知"
        print(item)

    async def run_parse(self, link, session):
        # crawler.warning('开始获取: {}'.format(link))
        # print('开始获取: {}'.format(link))
        source = await self.fetch(link, session)
        self.extract_elements(source)

    async def consumer(self, link):
        async with self.semaphore:
            conn = aiohttp.TCPConnector(verify_ssl=False)  # 防止ssl报错
            async with aiohttp.ClientSession(connector=conn) as session:  # 创建session
                try:
                    await self.run_parse(link, session)
                except:
                    pass

    async def run_consumer(self, redis_key):
        for md5 in self.redis_client.hgetall(redis_key):
            data_dict = self.get_redis_data(redis_key, md5)
            url = data_dict.get("url")
            await self.consumer(url)
            self.del_redis_str(redis_key, md5)

    def get_redis_data(self,redis_key,redis_str):
        md5 = redis_str.decode()
        data = self.redis_client.hget(redis_key,md5).decode()
        # print(data)
        data = re.sub("\'", '\"', data) # replace
        data_dict = json.loads(data)
        return data_dict

    def del_redis_str(self,redis_key, redis_str):
        redis_md5 = redis_str.decode()
        # print("DEL REDIS_MD5")
        return self.redis_client.hdel(redis_key,redis_md5)

    def run(self,redis_key):
        crawler.warning("REDIS KEY: " + redis_key)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.ensure_future(self.run_consumer(redis_key)))
        loop.close()

if __name__ == '__main__':
    L = A_1688_Detail()
    redis_key = "A1688_Url"
    L.run(redis_key)





# import requests
# proxy = {"http":"http://182.96.241.68:4249"}
# res= requests.get("https://shop1469552320392.1688.com/page/contactinfo.htm",headers=headers,proxies=proxy)
# print(res.text)
#
# "15019471212"
# "m-mobilephone"