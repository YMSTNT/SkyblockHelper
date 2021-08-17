
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from database import Database
from skyblock_api import SkyblockApi
from utils import Utils


class Downloader:

  @staticmethod
  def _put_data_to_db(datas, put_function):
    while datas:
      data = datas[-1]
      try:
        put_function(data)
        Utils.log(f'[{data["type"]}] Saved data', data['lastUpdated'])
        datas.pop()
      except sqlite3.OperationalError:
        Utils.log(f'[{data["type"]}] Database access failed', save=True)
        break
      except Exception as e:
        Utils.log(f'[{data["type"]}] Failed to put data to database', save=True)
        Utils.log(e, save=True)
        Utils.log(data, save=True)
        datas.pop()

  @staticmethod
  def _catch_input():
    while not Utils.quitting:
      inp = input()
      if inp == 'q':
        Utils.quitting = True
        Utils.log('Quitting...')
      elif inp == 'p':
        Utils.paused = not Utils.paused
        if Utils.paused:
          Utils.log('Pausing database access...')
        else:
          Utils.log('Resumed database access')

  @staticmethod
  def download_and_save_data():
    executor = ThreadPoolExecutor()
    bazaar_data, auction_data = [], []
    bazaar_future = executor.submit(SkyblockApi.get_new_bazaar, bazaar_data)
    auctions_future = executor.submit(SkyblockApi.get_new_ended_auctions, auction_data)
    executor.submit(Downloader._catch_input)
    should_quit = False
    while not should_quit:
      should_quit = Utils.quitting and not bazaar_data and not auction_data and bazaar_future.done() and auctions_future.done()
      if not Utils.paused:
        Database.connect()
        Downloader._put_data_to_db(bazaar_data, Database.insert_bazaar)
        Downloader._put_data_to_db(auction_data, Database.insert_auctions)
        sleep(0.1)
      else:
        Database.disconnect()
        Utils.sleep_while(lambda: Utils.paused, 10)
      if (queue := len(bazaar_data + auction_data)) > 2:
        Utils.log(f'{queue} datas are waiting in queue', save=True)

    Downloader._save_db()
