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
    base_uri = f'https://api.hypixel.net/skyblock/{uri}?key={SkyblockApi.key}'
    response = requests.get(f'{base_uri}/{uri}')
    if(response.status_code != 200):
      print("API failed: " + response.text)
      return None
    return json.loads(response.text)

  @staticmethod
  def _get_new(uri: str, delay: int):
    last_time = 0
    while True:
      data = SkyblockApi._get(uri)
      if(data and data['lastUpdated'] > last_time):
        last_time = data['lastUpdated']
        yield data
      sleep(delay)

  @staticmethod
  def get_new_bazaar():
    return SkyblockApi._get_new('bazaar', 5)

  @staticmethod
  def get_new_ended_auctions():
    return SkyblockApi._get_new('auctions_ended', 30)
