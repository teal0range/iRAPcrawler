# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from .items import *
from .io import *


class IrapcrawlerPipeline:
    entities = entityCollection()
    data_dict = {}
    io = MongoIO(scrapy.Spider.logger)

    def process_item(self, item, spider):
        if isinstance(item, entityItem):
            self.entities.append(dict(item.items()))
        elif isinstance(item, dict):
            if item['tag'] not in self.data_dict:
                self.data_dict[item['tag']] = {}
            self.data_dict[item['tag']].update(item['data'])
            if len(self.data_dict[item['tag']]) % 300 == 1:
                self.io.updateData(item['tag'], {"data": self.data_dict[item['tag']]})
                self.data_dict.clear()
        return item

    def close_spider(self, spider):
        if len(self.entities) != 0:
            self.io.updateData("entities", self.entities.getEntities())
        for key, val in self.data_dict.items():
            self.io.updateData(key, {"data": val})
