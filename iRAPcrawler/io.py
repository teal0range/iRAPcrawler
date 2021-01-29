import abc
import os
from abc import abstractmethod
import json
import pandas as pd
import pymongo


class BaseIO(metaclass=abc.ABCMeta):
    def __init__(self, logger):
        self.logger = logger

    @abstractmethod
    def saveData(self, key, df: pd.DataFrame):
        pass

    @abstractmethod
    def hasKey(self, key):
        pass

    @abstractmethod
    def readData(self, key) -> pd.DataFrame:
        pass

    @abstractmethod
    def readAllData(self):
        pass

    @abstractmethod
    def getAllKeys(self):
        pass

    @abstractmethod
    def remove(self, key):
        pass

    @abstractmethod
    def clear(self, prefix):
        pass


class MongoIO(BaseIO):

    def __init__(self, logger, config: dict = None):
        super().__init__(logger)
        if config is None:
            config = {
                "user": "admin",
                "password": "123456",
                "host": "127.0.0.1",
                "port": "27017",
                "db": "Irap",
            }
        self.config = config
        self.conn_str = "mongodb://{user}:{password}@{host}:{port}/{db}".format(**config)
        self.connection = pymongo.MongoClient(self.conn_str)

    def getConnection(self, key):
        client = self.connection[self.config['db']][key]
        return client

    def saveData(self, key, data):
        client = self.getConnection(key)
        key_client = self.getConnection(key + "_DataKeys")
        _ids = self.getDuplicateId(key)
        for item in _ids:
            if item in data:
                data['data'].pop(item)
        if len(data['data']) != 0:
            key_client.insert_many([{'_id': key} for key in data['data'].keys()])
            client.insert_many([{"_id": key, "data": val} for key, val in data['data'].items()])

    def updateData(self, key, data):
        self.saveData(key, data)

    def hasKey(self, key):
        return len(self.readDataKeys(key)) != 0

    def getDuplicateId(self, key):
        return self.readDataKeys(key)

    def readData(self, key) -> set:
        client = self.connection[self.config['db']][key]
        return {one["_id"] for one in client.find({}, {"_id": 1, "data": 0})}
        # return {one["_id"]: one["data"] for one in client.find()}

    def readDataKeys(self, key) -> set:
        client = self.connection[self.config['db']][key + "_DataKeys"]
        return {one["_id"] for one in client.find({}, {"_id": 1})}

    def readAllData(self):
        pass

    def getAllKeys(self):
        pass

    def remove(self, key):
        pass

    def clear(self, prefix):
        pass


class jsonIO(BaseIO):

    def __init__(self, logger, config=None):
        if config is None:
            config = {"data": "data"}
        super().__init__(logger=logger)
        self.dataPath = config['data']
        if not os.path.exists(os.path.join('Data', self.dataPath)):
            os.makedirs(os.path.join('Data', self.dataPath))

    def saveData(self, key, df):
        path = self.getDataPath(key)
        with open(path, 'w') as fp:
            json.dump(df, fp, indent=4, ensure_ascii=False)

    def updateData(self, key, df):
        path = self.getDataPath(key)
        try:
            dic = self.readData(key)
        except FileNotFoundError:
            dic = {'data': {}}
        dic['data'].update(df['data'])

        with open(path, 'w') as fp:
            json.dump(dic, fp, indent=4, ensure_ascii=False)
            self.logger.info("Current Data Length {}".format(len(dic['data'])))

    def hasKey(self, key):
        return os.path.exists(os.path.join(self.getDataPath(key)))

    def readData(self, key) -> dict:
        return json.load(open(self.getDataPath(key), 'r'))

    def remove(self, key):
        os.remove(self.getDataPath(key))

    def clear(self, prefix=""):
        keys = self.getAllKeys()
        for key in keys:
            if key.startswith(prefix):
                self.remove(key)

    def readAllData(self) -> dict:
        keys = self.getAllKeys()
        return {key: self.readData(key) for key in keys}

    def getAllKeys(self):
        path = os.path.join('Data', self.dataPath)
        for root, dirs, files in os.walk(path):
            return [file[:file.rfind(".json")] for file in files if file.endswith(".json")]

    def getDataPath(self, key):
        return os.path.join('Data', self.dataPath, key) + ".json"


if __name__ == '__main__':
    pass
