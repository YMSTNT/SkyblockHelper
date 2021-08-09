
from datetime import datetime

from database import Database
from skyblock_api import SkyblockApi


def epoch_to_human_time(epoch: int):
  return datetime.fromtimestamp(epoch / 1000).strftime('%Y-%m-%d %H:%M:%S')


def main():

  SkyblockApi.init()
  Database.init()
  # Database.setup(item_ids)

  for bazaar in SkyblockApi.get_new_bazaar():
    Database.insert_bazaar(bazaar)
    bazaar_time = epoch_to_human_time(bazaar['lastUpdated'])
    print(f'[{bazaar_time}] Saved bazaar data')


if __name__ == "__main__":
  main()
