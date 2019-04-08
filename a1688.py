# coding=utf-8
import hashlib
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
import re
import time
import pymongo
import random
from lxml import etree
import redis
from logger import crawler


class A1688():

    def __init__(self):
        self.chromeOptions = webdriver.ChromeOptions()
        # self.proxy_ip = ""
        # proxy = "--proxy-server=http://" + self.proxy_ip
        # self.chromeOptions.add_argument(proxy)
        self.browser = webdriver.Chrome(chrome_options=self.chromeOptions)
        # self.chromeOptions.add_argument('--log-level=5')
        self.wait = WebDriverWait(self.browser, 20)
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
        
        try:
            button_deal = self.browser.find_elements_by_css_selector('.sm-widget-sort.fd-clr.s-widget-sortfilt li')[1]
            button_deal.click()
        except:
            pass

        try:
            self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#offer60')))
        except:
            print('超时加载')
        for url in self.get_products():
            item["url"] = url
            # print(item)
            self.SaveUrl_to_redis(item)
        pages = self.get_page_num()
        if pages and pages > 1:
            print("Page Num:", pages)
            for page in range(2, pages + 1):
                crawler.warning("当前是第 " + str(page) + " 页")
                try:
                    self.get_more_page(page, item)
                    time.sleep(random.uniform(3,5))
                except Exception:
                    self.get_more_page(page, item)
                    time.sleep(random.uniform(4, 5))

    def get_page_num(self): # 获取页数
        try:
            doc = self.doc_xpath(self.browser.page_source)
            page_num = doc.xpath("//div[@class='fui-paging']/div/span[@class='fui-paging-total']/em/text()")[0]
            return int(page_num)
        except Exception as e:
            print(e)
            return None

    def doc_xpath(self, html):  # xpath
        return etree.HTML(html)

    def get_more_page(self, page, item): # 更多的页面
        page_input = self.wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "fui-paging-input")))
        page_input.clear()
        page_input.send_keys(page)
        try:
            page_button = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "fui-paging-btn")))
            page_button.click()
        except WebDriverException:
            try:
                page_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='fui_widget_5']/div/span[3]/button")))
                page_button.click()
            except:
                page_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#fui_widget_5 > div > span.fui-forward > button")))
                page_button.click()
        except Exception:
            page_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#fui_widget_5 > div > span.fui-forward > button")))
            page_button.click()

        # try:
        #     page_click = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#fui_widget_5 > span > a.fui-next")))
        #     if page_click:
        #         page_click.click()
        #     else:
        #         pass
        # except:
        #     pass

        time.sleep(random.uniform(2,3))
        # 向下拉动滚动条 1688这玩意属于ajax慢加载
        js = "var q=document.documentElement.scrollTop=2500"
        self.browser.execute_script(js)
        time.sleep(random.uniform(1,3))
        js = "var q=document.documentElement.scrollTop=4800"
        self.browser.execute_script(js)
        time.sleep(random.uniform(1,2))
        print('执行到底部')
        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#offer60')))
        except:
            print('超时加载')
        if self.get_products():
            for url in self.get_products():
                item["url"] = url
                # print(item)
                self.SaveUrl_to_redis(item)

    def get_products(self):
        try:
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
                except IndexError as e:
                    # crawler.warning("PARSE ERROR")
                    # print("PARSE ERROR:",e )
                    new_link = None
                # print(new_link)
                yield new_link
            print('一共{}条数据'.format(index)) #记录一下每一页有多少条信息
        except Exception:
            return None

    def get_md5(self, data): # 获取md5值
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
            crawler.warning("GET MD5 ERROR")
            # print("GET MD5 ERROR:",e)
            return None

    def SaveUrl_to_redis(self, data): # 保存到redis  hash去重
        try:
            if data and data["url"]:
                data_md5 = self.get_md5(data["url"])
                if data_md5:
                    if self.redis_client.hset("A1688_Url", data_md5, data):
                        print("成功存储到REDIS", data)
                    else:
                        print("存储URL已经存在", data)
        except Exception as e:
            print("SAVE TO REDIS ERROR:",e)
            pass

    def save_to_mongo(self, item, key):
        collection = self.db[key]
        if item:
            if collection.update({"": item[""]}, {"$set": item}, True):
                print("成功存储到MONGODB", item["name"])
            else:
                print("存储到MONGODB失败", item["name"])
    
    def get_redis_data(self,redis_str):  # get redis data  return
        md5 = redis_str.decode()
        data = self.redis_client.hget("A1688",md5)
        data_dict = json.loads(data.decode())
        return data_dict
       
    def del_redis_str(self, redis_str): # 删除
        redis_md5 = redis_str.decode()
        self.redis_client.hdel("A1688",redis_md5)
    
    def run(self):
        for md5 in self.redis_client.hgetall("A1688"):
            data_dict = self.get_redis_data(md5)
            key = data_dict.get("tag")
            print("DEL REDIS_TAG:", key)
            self.del_redis_str(md5)
            crawler.warning("当前tag: " + key)
            time.sleep(3)
            self.crawle(key, data_dict)


if __name__ == '__main__':
    A = A1688()
    A.run()

"https://s.1688.com/selloffer/offer_search.htm?keywords=%CA%D6%BB%FA&beginPage=5"
