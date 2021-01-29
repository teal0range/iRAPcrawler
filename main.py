from selenium import webdriver
import pandas as pd
import time
import os
import json
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import logging


def getLogger(name):
    if not os.path.exists("Log"):
        os.mkdir("Log")
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s][%(levelname)5s]   %(message)s')
    _logger = logging.getLogger(name)
    fh = logging.FileHandler(os.path.join('Log', '{}.log'.format(name)), mode='a', encoding='utf-8', delay=False)
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s][%(levelname)5s]   %(message)s")
    fh.setFormatter(fmt)
    _logger.handlers.append(fh)
    return _logger


def timeCounter(f):
    def inner(self, *args, **kwargs):
        t = time.time_ns()
        res = f(self, *args, **kwargs)
        self._logger.info("{} Cost {:.2f}s".format(f.__name__, (time.time_ns() - t) / 1e9))
        return res

    return inner


class Crawler:
    _logger = getLogger('crawler')

    def __init__(self, username, password):
        self._driver = webdriver.Chrome()
        self._driver.implicitly_wait(5)
        self._driver.get("https://global.irap.io/")
        self.username = username
        self.password = password
        self._data = []
        self._color_dict = {
            "1": "健康前景",
            "2": "亚健康",
            "3": "引起注意",
            "4": "重大风险",
        }
        self._pageCount = 1

        self.cookies = self.get_cookies()
        if self.cookies is not None:
            for cookie in self.cookies:
                self._driver.add_cookie(cookie)
                self._driver.refresh()

    @timeCounter
    def _signIn(self):
        if self.cookies is None:
            username = self._driver.find_element_by_css_selector("#userName")
            username.send_keys(self.username)
            password = self._driver.find_element_by_css_selector("#password")
            password.send_keys(self.password)
            submit = self._driver.find_element_by_css_selector("button[type='submit']")
            submit.click()
            time.sleep(5)
            with open("iRAPcrawler/spiders/cookies.json", 'w') as fp:
                fp.write(json.dumps(self._driver.get_cookies(), indent=4))
        self._entity()
        time.sleep(1)

    @staticmethod
    def get_cookies():
        if os.path.exists("iRAPcrawler/spiders/cookies.json"):
            with open("iRAPcrawler/spiders/cookies.json", 'r') as fp:
                cookie = json.load(fp)
            return cookie
        return None

    def _entity(self):
        self._driver.get(self._driver.current_url[:self._driver.current_url.rfind("/")] + "/entity")

    @timeCounter
    def _getListItem(self):

        item_xpath = "//*[@id='root']/div/div/div[2]/div[2]/div/div[1]/div/div/div[8]/div[1]/div/div/div/div/div/div[" \
                     "2]/table/tbody/tr"
        elem_list = self._driver.find_elements_by_xpath(item_xpath)
        for elem in elem_list:
            elem.click()
            item = self._parseItem(elem)
            dashboard = self._parseDashBoard()
            item.update(dashboard)
            self._logger.info(item)
            self._data.append(item)

    @timeCounter
    def _parseDashBoard(self) -> dict:
        res = {}
        x_path = r"//*[@class='signal-square signal-color-']"
        WebDriverWait(self._driver, timeout=60, poll_frequency=0.5, ignored_exceptions=None).until_not(
            EC.presence_of_element_located((By.XPATH, x_path))
        )
        time.sleep(1)
        text_list = self._driver.find_element_by_xpath("//*[@id='root']/div/div/div[2]/div[2]/div/"
                                                       "div[2]/div[1]/div[2]").text.split("\n")
        res['date'] = text_list[14]
        res['累计违约率(bps)'] = text_list[18]
        res['PDiR'] = text_list[24]
        res['危险度-地区'] = text_list[29]
        res['危险度-地区-行业'] = text_list[40]
        res['危险度-地区-行业 3M后'] = text_list[56]
        while True:
            try:
                res.update(self._parseSignal())
                break
            except KeyError:
                time.sleep(1)
                continue

        return res

    def _parseSignal(self):
        signals = self._driver.find_elements_by_css_selector(".signal-past")
        return {
            signal.text: self._color_dict[signal.find_element_by_css_selector("div:nth-child(1)").
                get_attribute("class")[-1]] for signal in signals
        }

    @timeCounter
    def _parseItem(self, elem) -> dict:
        item = {}
        text = elem.text.split("\n")
        signal = self._color_dict[elem.find_element_by_xpath("//td[3]/div/div").get_attribute("class")[-1]]
        item['company'], item['country'], item['signal'] = text[1], text[3], signal
        return item

    def _output(self):
        pd.DataFrame(self._data).to_csv("res.csv", index=False)

    def _nextPage(self):
        xpath = "//*[@id='root']/div/div/div[2]/div[2]/div/div[1]/div/div/div[8]/div[2]/a[2]"
        _next = self._driver.find_element_by_xpath(xpath)
        if _next.get_attribute("class").endswith("disabled"):
            return False
        else:
            _next = self._driver.find_element_by_xpath(xpath)
            self._pageCount += 1
            _next.click()
            ava_xpath = r"//*[@class='antd-pro\i-e-w-s\-n-k\-n-k.-web.-react-redux\-client-app\src\components" \
                        r"\-pagination\index-container '] "
            WebDriverWait(self._driver, 120, poll_frequency=1, ignored_exceptions=None).until(
                EC.presence_of_element_located((By.XPATH, ava_xpath))
            )
            self._logger.info("Page {}".format(self._pageCount))
            return True

    def run(self):
        self._signIn()
        flag = True
        while flag:
            self._getListItem()
            flag = self._nextPage()
            self._output()


if __name__ == '__main__':
    Crawler('syy@yzig.com.cn', '!QAZ2wsx').run()
