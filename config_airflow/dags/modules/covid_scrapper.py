import requests
import pandas as pd
import logging

class CovidScrapper():
    def __init__(self, url):
        self.url = url

    def get_data(self):
        response = requests.get(self.url)
        result = response.json()['data']['content']
        logging.info("GETT")
        df = pd.json_normalize(result)
        logging.info("DATA READY")
        return df