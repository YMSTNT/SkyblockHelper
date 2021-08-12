
import sqlite3
import sys
from datetime import datetime

from data_plotter import DataPlotter
from database import Database
from skyblock_api import SkyblockApi


def epoch_to_human_time(epoch: int):
  return datetime.fromtimestamp(epoch / 1000).strftime('%Y-%m-%d %H:%M:%S')


def main():
  DataPlotter.show_bazaar('BOOSTER_COOKIE', complex=True)
  return


def init():
  SkyblockApi.init()
  Database.init()
  # Database.setup()


def save_bazaar():
  bazaar_data = []
  for bazaar in SkyblockApi.get_new_bazaar(0 if bazaar_data else 5):
    if bazaar_data:
      print(len(bazaar_data), 'bazaar data is waiting for db')
    bazaar_data.insert(0, bazaar)
    tries = 5
    while bazaar_data and tries:
      bazaar = bazaar_data.pop()
      try:
        tries -= 1
        Database.insert_bazaar(bazaar)
        bazaar_time = epoch_to_human_time(bazaar['lastUpdated'])
        print(f'[{bazaar_time}] Saved bazaar data')
      except sqlite3.OperationalError as error:
        print('Bazaar failed: ', error)
        bazaar_data.append(bazaar)
        break
      except Exception as error:
        print('CRITICAL BAZAAR ERROR:', error)
        print('BAD BAZAAR DATA:', bazaar)


def save_auctions():
  auctions_data = []
  for auctions in SkyblockApi.get_new_ended_auctions(10 if auctions_data else 30):
    if auctions_data:
      print(len(auctions_data), 'auction data is waiting for db')
    auctions_data.insert(0, auctions)
    tries = 2
    while auctions_data and tries:
      auctions = auctions_data.pop()
      try:
        tries -= 1
        Database.insert_auctions(auctions)
        auctions_time = epoch_to_human_time(auctions['lastUpdated'])
        print(f'[{auctions_time}] Saved auctions data')
      except sqlite3.OperationalError as error:
        print('Auctions failed: ', error)
        auctions_data.append(auctions)
        break
      except Exception as error:
        print('CRITICAL AUCTIONS ERROR:', error)
        print('BAD AUCTIONS DATA:', auctions)


if __name__ == '__main__':
  init()
  if len(sys.argv) == 1:
    main()
  elif sys.argv[1] == 'save_bazaar':
    save_bazaar()
  elif sys.argv[1] == 'save_auctions':
    save_auctions()
