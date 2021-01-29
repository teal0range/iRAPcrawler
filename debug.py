from scrapy.cmdline import execute
import os
import sys
from iRAPcrawler.spiders.IRAP import *

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
execute(['scrapy', 'crawl', 'GETDATA'])

# if __name__ == '__main__':
#     process = CrawlerProcess()
#     process.crawl(RiskRegionIndustrySpider)
#     process.crawl(RiskRegionSpider)
#     process.start()
