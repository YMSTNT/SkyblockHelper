
import sys
from datetime import datetime

from data_plotter import DataPlotter
from database import Database
from skyblock_api import SkyblockApi


def epoch_to_human_time(epoch: int):
  return datetime.fromtimestamp(epoch / 1000).strftime('%Y-%m-%d %H:%M:%S')


def main():
  DataPlotter.show_bazaar('BOOSTER_COOKIE')
  return


def init():
  SkyblockApi.init()
  Database.init()
  # Database.setup(item_ids)


def save_bazaar():
  bazaar_data = []
  for bazaar in SkyblockApi.get_new_bazaar():
    bazaar_data.append(bazaar)
    tries = 3
    while bazaar_data and tries:
      try:
        tries -= 1
        Database.insert_bazaar(bazaar_data.pop())
        bazaar_time = epoch_to_human_time(bazaar['lastUpdated'])
        print(f'[{bazaar_time}] Saved bazaar data')
      except:
        print('FAILED TO ACCESS DATABASE')


if __name__ == "__main__":
  init()
  if len(sys.argv) == 1:
    main()
  elif sys.argv[1] == "save_bazaar":
    save_bazaar()
