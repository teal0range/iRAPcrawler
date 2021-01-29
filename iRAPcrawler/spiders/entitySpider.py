import scrapy
import json
from scrapy import Request
from ..items import *
from ..io import *


class EntitySpider(scrapy.Spider):
    name = 'entitySpider'
    cookies = r"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" \
              r".eyJpc3MiOiJDcmlhdCBBdXRob3JpemF0aW9uIFNlcnZlciIsImF1ZCI" \
              r"6WyJMeHgxSFhrc2lqV0N3TlJOSFNBNUJqUEo2RVI2VDlxd2I4UE9OSHRx" \
              r"OWpvPSIsIkNyaWF0IFJlc291cmNlIFNlcnZlciJdLCJqdGkiOiJmNmUyNTk" \
              r"yZC0wMWYxLTQxYTAtOTgzMC1hYzNiMTM1Nzk5YjQiLCJpYXQiOiIxLzI2Lz" \
              r"IwMjEgMjowODoyMiBBTSIsInN1YiI6InN5eUB5emlnLmNvbS5jbiIsInVuaX" \
              r"F1ZV9uYW1lIjoic3l5QHl6aWcuY29tLmNuIiwibmJmIjoxNjExNjI2OTAyLCJl" \
              r"eHAiOjE2MTE2NTU3MDJ9.c2Yzp8_rty3CqkBU4sJq4qgHEEqIcYoO1HiDBkkZjmw "

    def start_requests(self):
        with open("iRAPcrawler/spiders/headers.json", 'r') as fp:
            headers_dict = json.load(fp)
        val = headers_dict['GETENTITY']
        print(val)
        val['headers']["authorization"] = "Bearer {}".format(self.cookies)
        val['headers']["cookies"] = "antd-pro-authority=ROLE_IEWS_ANALYST;Authentication={}".format(self.cookies)
        body = "{\"method\":\"GET\",\"url\":\"/iews/entities/sectorbased/?apikey=TEST1&pageno=%s" \
               "&pagesize=100&lang=zh-CN\",\"timeout\":3000000} "
        for i in range(2):
            yield Request(method=val['method'],
                          url=val['url'],
                          callback=self.parse,
                          headers=val['headers'],
                          body=body % (i + 1))

    def parse(self, response, **kwargs):
        items = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))['items']
        res = []
        for item in items:
            s = entityItem()
            s['lad'] = item['lad']
            s['code'] = item['code']
            s['name'] = item['name']
            s['globalizedName'] = item['globalizedName']
            s['pd'] = item['pd']
            s['pdirname'] = item['pdirname']

            s['BASIC_REGION_ID'] = item["basicRegionCard"]['id']
            s['BASIC_REGION_NAME'] = item["basicRegionCard"]['name']
            s['PRIMARY_REGION_ID'] = item["primaryRegionCard"]['id']
            s['PRIMARY_REGION_NAME'] = item["primaryRegionCard"]['name']
            s['BASIC_INDUSTRY_ID'] = item["basicIndustryCard"]['id']
            s['BASIC_INDUSTRY_NAME'] = item["basicIndustryCard"]['name']

            res.append(s)
        return res
