# coding=utf-8
import hashlib
import json

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import re
import time
import pymongo
import random
from lxml import etree
import redis


class A1688():

    def __init__(self):
        self.chromeOptions = webdriver.ChromeOptions()
        # self.proxy_ip = ""
        # proxy = "--proxy-server=http://" + self.proxy_ip
        # self.chromeOptions.add_argument(proxy)
        self.browser = webdriver.Chrome(chrome_options=self.chromeOptions)
        self.wait = WebDriverWait(self.browser, 15)
        self.browser.set_window_size(1400, 900)
        self.client = pymongo.MongoClient('localhost')
        self.db = self.client['A1688']
        self.redis_client = redis.Redis(host="localhost", port=6379, db=2)

    def crawle(self, key, item):
        self.browser.get('https://www.1688.com/')
        try:
            identity_cancel = self.wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "identity-cancel")))
            identity_cancel.click()
        except:
            pass
        input_keys = self.wait.until(
            EC.presence_of_element_located((By.ID, "alisearch-keywords")))
        input_keys.send_keys(key)
        submit_button = self.wait.until(
            EC.element_to_be_clickable((By.ID, "alisearch-submit")))
        submit_button.click()
        try:
            button_1 = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "s-overlay-close-l")))
            button_1.click()
        except:
            pass

        button_deal = self.browser.find_elements_by_css_selector('.sm-widget-sort.fd-clr.s-widget-sortfilt li')[1]
        button_deal.click()

        try:
            self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#offer60')))
        except:
            print('*' * 30, '超时加载', '*' * 30)
        for url in self.get_products():
            item["url"] = url
            # print(item)
            self.SaveUrl_to_redis(item)
        page = self.get_page_num()
        if page and page > 1:
            print("Page Num:", page)
            for page in range(2, page + 1):
                self.get_more_page(page, item)
                import random
                time.sleep(random.uniform(1,2))

    def get_page_num(self):
        try:
            doc = self.doc_xpath(self.browser.page_source)
            page_num = doc.xpath("//div[@class='fui-paging']/div/span[@class='fui-paging-total']/em/text()")[0]
            return int(page_num)
        except Exception as e:
            print(e)
            return None

    def doc_xpath(self, html):
        return etree.HTML(html)

    def get_more_page(self, page, item):
        page_input = self.wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "fui-paging-input")))
        page_input.clear()
        page_input.send_keys(page)
        page_button = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "fui-paging-btn")))
        page_button.click()
        # 并没有起作用
        # browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
        # 非常重要，让浏览器缓过来
        time.sleep(2)
        # 起作用，因为broswer反应过来了
        self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
        time.sleep(1)
        self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
        print('执行到底部')
        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#offer60')))
        except:
            print('*' * 30, '超时加载', '*' * 30)
        for url in self.get_products():
            item["url"] = url
            # print(item)
            self.SaveUrl_to_redis(item)

    def get_products(self):
        html = self.browser.page_source
        doc = etree.HTML(html)
        lis = doc.xpath("//ul[@id='sm-offer-list']/li")
        index = 0
        for li in lis:
            try:
                link = li.xpath(".//span[@class='sm-offer-companyTag sw-dpl-offer-companyTag']/a/@href")[0]
                link_split =link.split('page/creditdetail')
                new_link = link_split[0] + "page/contactinfo.htm"
                index +=1
            except IndexError:
                new_link = None
            # print(new_link)
            yield new_link

        print('	(●ˇ∀ˇ●)	' * 5)
        print('一共%d条数据' % index)

    def get_md5(self, data):
        md5 = hashlib.md5()
        try:
            if isinstance(data, dict) or isinstance(data, list):
                md5.update(json.dumps(data).encode("utf-8")) # md5.hexdigest()[8:-8]  16bytes
            elif isinstance(data, str):
                md5.update(data.encode("utf-8")) # md5.hexdigest()[8:-8]  16bytes
            else:
                pass
            return md5.hexdigest()
        except Exception as e:
            print("GET MD5 ERROR:",e)
            return None

    def SaveUrl_to_redis(self, data):
        try:
            if data:
                data_md5 = self.get_md5(data["url"])
                if data_md5:
                    if self.redis_client.hset("A1688_Url", data_md5, data):
                        print("成功存储到REDIS", data)
                    else:
                        print("存储到REDIS失败", data)
        except Exception as e:
            print(e)
            pass

    def save_to_mongo(self, item, key):
        collection = self.db[key]
        if item:
            if collection.update({"": item[""]}, {"$set": item}, True):
                print("成功存储到MONGODB", item["name"])
            else:
                print("存储到MONGODB失败", item["name"])
    
    def get_redis_data(self,redis_str):
        md5 = redis_str.decode()
        data = self.redis_client.hget("A1688",md5)
        data_dict = json.loads(data.decode())
        return data_dict
            
    def del_redis_str(self, redis_str):
        redis_md5 = redis_str.decode()
        print("DEL REDIS_MD5")
        self.redis_client.hdel("A1688",redis_md5)
    
    def run(self):
        for md5 in self.redis_client.hgetall("A1688"):
            data_dict = self.get_redis_data(md5)
            key = data_dict.get("tag")
            time.sleep(3)
            self.crawle(key, data_dict)
            self.del_redis_str(md5)

if __name__ == '__main__':
    A = A1688()
    A.run()

