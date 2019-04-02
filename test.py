# coding=utf-8
import asyncio
import json
import re
import aiohttp
import aiomysql
from lxml import etree
import redis
from aiomultiprocess import Pool
from logger import crawler

class A_1688_Detail():
    def __init__(self):
        self.MAX_C = 100
        self.semaphore = asyncio.Semaphore(self.MAX_C)  # 控制并发数
        crawler.warning("控制并发数 {}".format(str(self.MAX_C)))
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
                        'Cookie': '__sw_newuno_count__=1; UM_distinctid=16755542bb76fb-03f74e3e7fd584-3c720356-1fa400-16755542bb8171; cna=4CMQFCJ961sCAXFDCsi/YcZ/; cookie2=1451a0d6107219a647bd8749bb850ee6; t=8479bafcf0d1952ad87b3970685ad3a9; _tb_token_=536063d81506b; lid=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389; ali_ab=116.22.163.179.1543510145732.0; ali_apache_track=c_mid=b2b-2992790576ee171|c_lid=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389|c_ms=1; ali_apache_tracktmp=c_w_signed=Y; hng=CN%7Czh-CN%7CCNY%7C156; alisw=swIs1200%3D1%7C; _bl_uid=8zjOgtOLomj4nXraRvaL7mdyqvFL; ali_beacon_id=58.62.93.181.1553505124500.600151.4; __last_loginid__=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389; __cn_logon_id__=%E4%BD%A0%E8%84%91%E5%AD%90%E8%BF%9B%E6%B0%B4%E4%BA%8614322389; __cn_logon__=true; h_keys="%u5b55%u5987%u62a4%u80a4#%u667a%u80fd%u624b%u73af#%u53ea%u80fd%u624b%u73af#%u53d8%u9891%u5668#%u8c46%u80c6%u706f#%u9a91%u884c%u670d#%u9ad8%u9886%u6bdb%u8863#%u7535%u963b%u5668#%u4e8c%u624b%u624b%u673a#%u624b%u673a"; alicnweb=touch_tb_at%3D1554188376601%7ChomeIdttS%3D74872396776817612388535400667698430328%7ChomeIdttSAction%3Dtrue%7Clastlogonid%3D%25E4%25BD%25A0%25E8%2584%2591%25E5%25AD%2590%25E8%25BF%259B%25E6%25B0%25B4%25E4%25BA%258614322389%7Cshow_inter_tips%3Dfalse%7Chp_newbuyerguide%3Dtrue; cookie1=UIHzmwsRGZPI2P5rNHO2lX0w3R0Kxa2ZZqtR0lsiXMs%3D; cookie17=UUGrdCbj1XPhCQ%3D%3D; sg=96d; csg=1b1ba29f; unb=2992790576; _nk_=%5Cu4F60%5Cu8111%5Cu5B50%5Cu8FDB%5Cu6C34%5Cu4E8614322389; last_mid=b2b-2992790576ee171; _csrf_token=1554189230202; ad_prefer="2019/04/02 15:13:52"; _is_show_loginId_change_block_=b2b-2992790576ee171_false; _show_force_unbind_div_=b2b-2992790576ee171_false; _show_sys_unbind_div_=b2b-2992790576ee171_false; _show_user_unbind_div_=b2b-2992790576ee171_false; __rn_alert__=false; l=bBP3u7VcvscOl3DzBOCNVuIRhCQtSIRYSuPRwRjBi_5N-986tZ_Olij9nhJ62j5R_zLp4mebW5y9-etFs; isg=BDc3ySO0J63eXqR_PbM-tJJoxiJBVAFxuKHY-4nkW4ZtOFd6kM1trldSG9jD1ePW'
                        }
        self.redis_client = redis.Redis(host="119.23.140.162", port=6379, db=2, password="caonima")


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
            # print(url)
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
