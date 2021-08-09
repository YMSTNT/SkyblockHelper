import json
from time import sleep

import requests


class SkyblockApi:

  @staticmethod
  def init():
    with open('api_key.txt') as file:
      SkyblockApi.key = file.readline()

  @staticmethod
  def _get(uri: str):
    base_uri = 'https://api.hypixel.net'
    response = requests.get(f'{base_uri}/{uri}')
    if(response.status_code != 200):
      print("API failed: " + response.text)
      return None
    return json.loads(response.text)

  @staticmethod
  def get_bazaar():
    return SkyblockApi._get('/skyblock/bazaar')

  @staticmethod
  def get_bazaar_prices():
    bazaar = SkyblockApi.get_bazaar()
    if(not bazaar):
      return None
    bazaar_prices = dict()
    bazaar_prices['lastUpdated'] = bazaar['lastUpdated']
    product_prices = dict()
    for product in bazaar['products'].values():
      quick_status = product['quick_status']
      product_price = dict()
      product_price['sell'] = quick_status['sellPrice']
      product_price['buy'] = quick_status['buyPrice']
      product_prices[quick_status['productId']] = product_price
    bazaar_prices['products'] = product_prices
    return bazaar_prices

  @staticmethod
  def get_new_bazaar_prices():
    last_time = 0
    while True:
      prices = SkyblockApi.get_bazaar_prices()
      if(prices and prices['lastUpdated'] > last_time):
        last_time = prices['lastUpdated']
        yield prices
      sleep(5)
