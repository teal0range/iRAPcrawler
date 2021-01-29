# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class IrapcrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class entityItem(scrapy.Item):
    lad = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    globalizedName = scrapy.Field()
    pd = scrapy.Field()
    pdirname = scrapy.Field()
    INDUSTRY_RANKNUMBER = scrapy.Field()
    REGION_RANKNUMBER = scrapy.Field()
    REGION_INDUSTRY_RANKNUMBER = scrapy.Field()
    BASIC_REGION_ID = scrapy.Field()
    BASIC_REGION_NAME = scrapy.Field()
    PRIMARY_REGION_ID = scrapy.Field()
    PRIMARY_REGION_NAME = scrapy.Field()
    BASIC_INDUSTRY_ID = scrapy.Field()
    BASIC_INDUSTRY_NAME = scrapy.Field()


class entityCollection:
    def __init__(self):
        self.entities = []

    def append(self, item):
        self.entities.append(item)

    def getEntities(self):
        return {"data": self.entities}

    def __len__(self):
        return len(self.entities)
