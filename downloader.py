
from concurrent.futures import ThreadPoolExecutor
from time import sleep, time
from traceback import format_exc

import numpy
from scipy import stats

from data_utils import DataUtils
from database import Database
from my_queue import Queue
from skyblock_api import SkyblockApi
from utils import Utils


class Downloader:

  @staticmethod
  def _put_data_to_db(datas: Queue, put_function):
    while datas:
      data = datas.peek()
      try:
        put_function(data)
        Utils.log(f'[{data["type"]}] Saved data', data['lastUpdated'])
        datas.dequeue()
      except Exception:
        Utils.log(f'[{data["type"]}] Failed to put data to database', save=True)
        Utils.log(format_exc(), save=True)
        Utils.log(data, save=True)
        datas.dequeue()

  @staticmethod
  def _save_data():
    should_quit = False
    while not should_quit:
      should_quit = Utils.quitting and not Downloader.bazaar_data and not Downloader.auction_data and Downloader.bazaar_task.done() and auctions_task.done()
      if not Utils.paused:
        Database.connect()
        Downloader._put_data_to_db(Downloader.bazaar_data, Database.insert_bazaar)
        Downloader._put_data_to_db(Downloader.auction_data, Database.insert_auctions)
      else:
        Database.disconnect()
        Utils.sleep_while(lambda: Utils.paused, 10)
      if (len_datas := len(Downloader.bazaar_data) + len(Downloader.auction_data)) > 2:
        Utils.log(f'{len_datas} datas are waiting in queue', save=True)

  @staticmethod
  def download_and_save_data(executor: ThreadPoolExecutor):
    Downloader.bazaar_data = Queue()
    Downloader.auction_data = Queue()
    bazaar_task = executor.submit(SkyblockApi.get_new_bazaar, Downloader.bazaar_data)
    auctions_task = executor.submit(SkyblockApi.get_new_ended_auctions, Downloader.auction_data)
    executor.submit(Downloader._save_data)
