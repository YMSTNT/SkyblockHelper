import json
from time import sleep

import requests

from utils import Utils


class SkyblockApi:

  @staticmethod
  def init():
    with open('data/api_key.txt') as file:
      SkyblockApi.key = file.readline()

  @staticmethod
  def _get(uri: str):
    base_uri = f'https://api.hypixel.net/skyblock/{uri}?key={SkyblockApi.key}'
    try:
      response = requests.get(f'{base_uri}/{uri}')
      if response.status_code != 200:
        Utils.log("API failed: " + response.text)
        return None
      return json.loads(response.text)
    except ConnectionError:
      return None
    except:
      Utils.log('GET REQUEST FAILED')

  @staticmethod
  def _get_new(uri: str, buffer: list, delay: int, log_type: str):
    last_time = 0
    while not Utils.quitting:
      data = SkyblockApi._get(uri)
      if(data and data['lastUpdated'] > last_time):
        data['type'] = log_type
        last_time = data['lastUpdated']
        Utils.log(f'[{data["type"]}] Downloaded data', data['lastUpdated'])
        buffer.insert(0, data)
      for _ in range(delay * 10):
        if Utils.quitting:
          break
        sleep(0.1)
    Utils.log(f'[{data["type"]}] Stopped downloading')

  @staticmethod
  def get_new_bazaar(buffer: list):
    return SkyblockApi._get_new('bazaar', buffer, 5, 'bz')

  @staticmethod
  def get_new_ended_auctions(buffer: list):
    return SkyblockApi._get_new('auctions_ended', buffer, 30, 'ah')
