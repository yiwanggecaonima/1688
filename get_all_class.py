# coding=utf-8
import time

import requests
from lxml import etree
import selenium.webdriver
web = selenium.webdriver.Chrome()

def get_html(url):
    # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
    # try:
    #     r = requests.get(url, headers=headers)
    #     r.encoding = r.apparent_encoding
    #     if r.status_code in [200,201]:
    #         return r.text
    #     return None
    # except Exception as e:
    #     print(e)
    #
    #     return None
    web.get(url)
    time.sleep(18)
    return web.page_source

def parse_class(html):
    doc = doc_xpath(html)
    divs = doc.xpath("//div[@class='floatLayer fd-clr']/div[@class='floatLayer_text fd-left floatLayer_text_new']/div")
    print(len(divs))
    for div in divs:
        # print(div)
        item = {}
        for i in div.xpath("./div"):
            h2 = i.xpath(".//h2/a/text()")[0]
            for lis in i.xpath("./ul/li"):
                for li in lis:
                    # print(li)
                    for a in li.xpath(".//a"):
                        # print(a)
                        a_text = a.xpath("./text()")[0]
                        item["class"] = h2
                        item["tag"] = a_text
                        print(item)

def doc_xpath(html):
    return etree.HTML(html)

def main():
    html = get_html("http://www.1688.com")
    if html:
        parse_class(html)


if __name__ == '__main__':
    main()

