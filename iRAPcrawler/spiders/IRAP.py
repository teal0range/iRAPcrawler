import scrapy
from scrapy import Request
import json
import time
from .cookies import Crawler
from ..io import *
import sys


class IrapSpider(scrapy.Spider):
    name = 'irap'
    crawler = Crawler('syy@yzig.com.cn', '!QAZ2wsx')
    cookies = crawler.get_cookies()['value']
    expiry = crawler.get_cookies()['expiry']
    io = jsonIO(scrapy.Spider.logger, config={"data": "entity"})
    _io = MongoIO(scrapy.Spider.logger)
    cnt = 1

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.API = self.name
        self.entities = self.getEntities()
        self.keys = self.getCrawled()

    def getEntities(self):
        data = self.io.readData("entities")['data']
        return [item['code'] for item in data]

    def getCrawled(self):
        return self._io.readDataKeys(self.name)

    def updateCookie(self):
        if time.time_ns() / 1e9 > self.expiry + 100:
            self.cookies = self.crawler.get_cookies()['value']
            self.expiry = self.crawler.get_cookies()['expiry']

    def start_requests(self):
        with open("iRAPcrawler/spiders/headers.json", 'r') as fp:
            headers_dict = json.load(fp)
        val = headers_dict[self.API]
        self.updateCookie()
        val['headers']["authorization"] = "Bearer {}".format(self.cookies)
        val['headers']["cookies"] = "antd-pro-authority=ROLE_IEWS_ANALYST;Authentication={}".format(self.cookies)

        for entity in self.entities:
            if entity in self.keys:
                continue
            yield Request(method=val['method'],
                          url=val['url'],
                          callback=self.parse,
                          headers=val['headers'],
                          body=val['body'] % entity)
            # if self.cnt < 2:
            #     self.cnt += 1
            # else:
            #     break

    def parse(self, response, **kwargs):
        return {"tag": self.API, "data": json.loads(response.text.strip(r'"').replace(r'\"', r'"'))}


class RiskRegionSpider(IrapSpider):
    name = 'RISK_REGION'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["entityCode"]: s['data'][0]}}


class RiskRegionIndustrySpider(IrapSpider):
    name = 'RISK_REGION_INDUSTRY'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["entityCode"]: s['data'][0]}}


class CumDefaultRateSpider(IrapSpider):
    name = 'CUM_Default_Rate'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["objectCode"]: s['data'][0]}}


class RiskFactor(IrapSpider):
    name = 'RiskFactor'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["objectCode"]: s['data'][0]}}


class DefaultRateTimeSeries(IrapSpider):
    name = 'DefaultRateTimeSeries'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["objectCode"]: s['data'][0]}}


class RISK_FACTOR(IrapSpider):
    name = 'RISK_FACTOR'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["objectCode"]: s['data'][0]}}


class DEFAULT_STRUCTURE(IrapSpider):
    name = 'DEFAULT_STRUCTURE'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["objectCode"]: s['data'][0]}}


class GETDATA(IrapSpider):
    name = 'GETDATA'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        # if len(s['data']) == 0:
        #     return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {1: s}}


class DEFAULT_ANALYSIS(IrapSpider):
    name = 'DEFAULT_ANALYSIS'

    def parse(self, response, **kwargs):
        s = json.loads(response.text.strip(r'"').replace(r'\"', r'"'))
        if len(s['data']) == 0:
            return {"tag": self.API, "data": {}}
        return {"tag": self.API, "data": {s['data'][0][0]["objectCode"]: s['data'][0]}}
