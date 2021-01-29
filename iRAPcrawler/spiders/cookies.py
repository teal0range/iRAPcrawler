from selenium import webdriver
import time
import os
import json
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


def timeCounter(f):
    def inner(self, *args, **kwargs):
        t = time.time_ns()
        res = f(self, *args, **kwargs)
        self._logger.info("{} Cost {:.2f}s".format(f.__name__, (time.time_ns() - t) / 1e9))
        return res

    return inner


class Crawler:
    # _logger = getLogger('crawler')

    def __init__(self, username, password):
        self._driver = None
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

        self.cookies = None
        self.cookies = self.get_cookies()

    def _signIn(self):
        self._driver = webdriver.Chrome()
        self._driver.get("https://global.irap.io/")
        WebDriverWait(self._driver, timeout=60, poll_frequency=0.5, ignored_exceptions=None).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#userName"))
        )
        if self.cookies is None:
            username = self._driver.find_element_by_css_selector("#userName")
            username.send_keys(self.username)
            password = self._driver.find_element_by_css_selector("#password")
            password.send_keys(self.password)
            submit = self._driver.find_element_by_css_selector("button[type='submit']")
            submit.click()
            time.sleep(5)
            with open("cookies.json", 'w') as fp:
                fp.write(json.dumps(self._driver.get_cookies(), indent=4))
        self._driver.close()
        time.sleep(1)

    def get_cookies(self):
        if not os.path.exists("cookies.json"):
            self._signIn()
        with open("cookies.json", 'r') as fp:
            cookies = json.load(fp)
        return cookies[0]
