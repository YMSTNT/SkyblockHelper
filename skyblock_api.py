import json
from time import sleep

import requests


class SkyblockApi:

  @staticmethod
  def init():
    with open('data/api_key.txt') as file:
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
  def get_new_bazaar():
    last_time = 0
    while True:
      bazaar = SkyblockApi.get_bazaar()
      if(bazaar and bazaar['lastUpdated'] > last_time):
        last_time = bazaar['lastUpdated']
        yield bazaar
      sleep(5)
