# coding=utf-8
import asyncio
import json
import re
import aiohttp
import aiomysql
import pymongo
from lxml import etree
import redis
from aiomultiprocess import Pool
from logger import crawler
import aioredis

# #把原来的hash格式数据转换成list队列  很耗时间 以后不能这么干了
# redis_client = redis.Redis(host="127.0.0.1", port=6379, db=6, password=None)
# for i in redis_client.hvals("A1688_Url"):
#     redis_client.lpush("A1688",i.decode())


'''
改成list之后利用rpop就可以一个一个的往外送 使用while 1来进行异步的调度
还使用了aioredis这个库 不过由于redis在本地 而且网站也会有延迟 所以没啥区别 并发也不大
'''
class A_1688_Detail():
    def __init__(self):
        self.MAX_C = 100
        self.semaphore = asyncio.Semaphore(self.MAX_C)  # 控制并发数
        crawler.warning("控制并发数 {}".format(str(self.MAX_C)))
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
                        'Cookie': 'UM_distinctid=16755542bb76fb-03f74e3e7fd584-3c720356-1fa400-16755542bb8171; cna=4CMQFCJ961sCAXFDCsi/YcZ/; cookie2=1451a0d6107219a647bd8749bb850ee6; t=8479bafcf0d1952ad87b3970685ad3a9; _tb_token_=536063d81506b; lid=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389; ali_ab=116.22.163.179.1543510145732.0; ali_apache_track=c_mid=b2b-2992790576ee171|c_lid=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389|c_ms=1; ali_apache_tracktmp=c_w_signed=Y; hng=CN%7Czh-CN%7CCNY%7C156; ali_beacon_id=58.62.93.181.1553505124500.600151.4; __last_loginid__=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389; __cn_logon_id__=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389; __cn_logon__=true; h_keys="%u589e%u7a20%u5242#%u51c0%u6c34%u673a#%u51c0%u6c34%u5668%u5bb6%u7528#%u6c34%u9f99%u5934%u51c0%u6c34%u5668#%u6237%u5916%u51c0%u6c34%u5668#%u6253%u5370%u673a#%u590d%u5370%u673a#%u667a%u80fd%u624b%u73af#%u51c0%u6c34%u5668#%u56f4%u5634%u56f4%u515c"; ad_prefer="2019/04/04 13:52:52"; alicnweb=touch_tb_at%3D1555033623086%7ChomeIdttS%3D74872396776817612388535400667698430328%7ChomeIdttSAction%3Dtrue%7Clastlogonid%3D%25E4%25BD%25A0%25E8%2584%2591%25E5%25AD%2590%25E8%25BF%259B%25E6%25B0%25B4%25E4%25BA%258614322389%7Cshow_inter_tips%3Dfalse%7Chp_newbuyerguide%3Dtrue; cookie1=UIHzmwsRGZPI2P5rNHO2lX0w3R0Kxa2ZZqtR0lsiXMs%3D; cookie17=UUGrdCbj1XPhCQ%3D%3D; sg=96d; csg=bfad535d; unb=2992790576; _nk_=%5Cu4F60%5Cu8111%5Cu5B50%5Cu8FDB%5Cu6C34%5Cu4E8614322389; last_mid=b2b-2992790576ee171; _csrf_token=1555033728053; _is_show_loginId_change_block_=b2b-2992790576ee171_false; _show_force_unbind_div_=b2b-2992790576ee171_false; _show_sys_unbind_div_=b2b-2992790576ee171_false; _show_user_unbind_div_=b2b-2992790576ee171_false; __rn_alert__=false; isg=BKioDsNQYORwF0syVm7ZEQmteZ96eQb0s0S3dmLZ3yMWvUgnC-Ikaz70tBXotsSz; l=bBP3u7VcvscOlEe0BOCwmuIRhCQTAIRYjuPRwRjBi_5B3_T1Sz_OlMVf-E962j5ROGTp4mebW5y9-etev'
                        }
        self.redis_client = redis.Redis(host="127.0.0.1", port=6379, db=6, password=None)
        self.client = pymongo.MongoClient('127.0.0.1')
        self.db = self.client['A1688']


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

    def extract_elements(self, source, key):
        doc = self.doc_xpath(source)
        item = {}
        item["key"] = key
        title = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='contact-info']/h4/text()")
        item["title"] = title[0] if len(title) > 0 else "标题未知"
        name = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='contact-info']/dl/dd/a[@class='membername']/text()")
        item["name"] = name[0] if len(name) > 0 else "联系人未知"
        address = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='fd-line']/div[@class='contcat-desc']//dd[@class='address']/text()")
        item["address"] = address[0].replace('\n','').replace(' ','') if len(address) > 0 else "地址未知"

        web = doc.xpath("//div[@class='contcat-desc']/dl/dd/div/a[@class='subdomain']/text()")
        item["web"] = web[0] if len(web) > 0 else "公司主页 未知"

        tels = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='fd-line']/div[@class='contcat-desc']/dl[1]/dd/text()")
        long_or_short = tels[0] if len(tels) > 0 else "电话未知"
        if long_or_short and '\n' in long_or_short:
            tel = long_or_short.replace('\n', '').replace(' ','')
        else:
            tel = long_or_short
        item["tel1"] = tel

        tel_and_others = doc.xpath("//div[@class='props-part']/div[@class='fd-clr']/div[@class='fd-line']/div[@class='contcat-desc']/dl[2]/dd/text()")
        tel_and_others = tel_and_others[0] if len(tel_and_others) > 0 else None
        if tel_and_others and '\n' in tel_and_others:
            tel_and_other = tel_and_others.replace('\n', '').replace(' ', '')
        else:
            tel_and_other = tel_and_others
        if tel_and_other:
            item["tel_and_other"] = tel_and_other
        else:
            try_tel_and_other = doc.xpath("//div[@class='m-content']/dl[@class='m-mobilephone']/dd/text()")
            item["tel_and_other"] = try_tel_and_other[0].replace('\n', '') if len(tel_and_other) > 0 else "移动电话或其他未知"
        # print(item)
        self.save_to_mongodb(item,key)

    async def run_parse(self, link, session, key):
        # crawler.warning('开始获取: {}'.format(link))
        # print('开始获取: {}'.format(link))
        source = await self.fetch(link, session)
        self.extract_elements(source, key)

    async def consumer(self, link, key):
        async with self.semaphore:
            conn = aiohttp.TCPConnector(verify_ssl=False)  # 防止ssl报错
            async with aiohttp.ClientSession(connector=conn) as session:  # 创建session
                try:
                    await self.run_parse(link, session, key)
                except:
                    pass

    async def run_consumer(self, redis_key):
        while 1:
            try:
                # b_data = self.redis_client.rpop(redis_key)
                # print(b_data)
                b_data = await self.connect_uri(redis_key)
                # print(b_data)
                data_dict = self.get_redis_data(b_data)
                # print(data_dict)
                url = data_dict.get("url")
                print(url)
                key = data_dict.get("key")
                await self.consumer(url,key)
            except ValueError:
                print("xpath结果为空！")
                pass

            except Exception as e:
                # print(redis_key)
                # print(self.redis_client.keys())
                if redis_key.encode() not in self.redis_client.keys():  # 获取url完毕
                    print("爬取结束,关闭爬虫！")
                    break

    def save_to_mongodb(self, item, key):
        collection = self.db[key]
        if item:
            if collection.update({"title": item["title"]}, {"$set": item}, True):
                print("成功存储到MONGODB", item)
            else:
                print("存储到MONGODB失败", item)

    async def connect_uri(self, redis_key):
        conn = await aioredis.create_connection("redis://127.0.0.1:6379/6")
        val = await conn.execute('rpop', redis_key)
        print(val.decode())
        return val.decode()

    def get_redis_data(self,str_data):
        data = re.sub("\'", '\"', str_data) # replace
        data_dict = json.loads(data)
        return data_dict

    def run(self,redis_key):
        crawler.warning("REDIS KEY: " + redis_key)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.ensure_future(self.run_consumer(redis_key)))
        loop.close()

if __name__ == '__main__':
    L = A_1688_Detail()
    redis_key = "A1688" # "queue_1688"
    L.run(redis_key)

