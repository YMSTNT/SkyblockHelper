
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from time import sleep, time

import numpy
from scipy import stats

from data_utils import DataUtils
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
  def _update_auction_price_for_id(id: str):
    # get relevant prices, calculate average
    now = int(time() * 1000)
    three_days_ago = now - DataUtils.DAY * 3
    query = Database.execute(f'SELECT price, count FROM EndedAuctions WHERE name = "{id}" AND time > {three_days_ago}')
    prices = [r[0] / r[1] for r in query]
    mode = int(stats.mode(prices)[0])
    median = numpy.median(prices)
    average_price = (mode + median) / 2
    # insert average to db
    Database.execute(f'UPDATE AuctionPrices SET price = {average_price}, time = {now} WHERE name = "{id}"')
    Utils.log(f'[ah] Updated price for {id}', now)

  @staticmethod
  def _update_auction_prices():
    while True:
      query = Database.execute('SELECT name, price, time FROM AuctionPrices WHERE priority > 0')
      auction_prices = [{'name': r[0], 'price': r[1], 'time': r[2]} for r in query]
      auction_prices.sort(key=lambda k: k['time'] or 0)
      for price_data in auction_prices:
        yield Downloader._update_auction_price_for_id(price_data['name'])
      yield

  @staticmethod
  def download_and_save_data():
    executor = ThreadPoolExecutor()
    bazaar_data, auction_data = [], []
    bazaar_future = executor.submit(SkyblockApi.get_new_bazaar, bazaar_data)
    auctions_future = executor.submit(SkyblockApi.get_new_ended_auctions, auction_data)
    executor.submit(Downloader._catch_input)
    update_auction_prices = Downloader._update_auction_prices()
    should_quit = False
    while not should_quit:
      should_quit = Utils.quitting and not bazaar_data and not auction_data and bazaar_future.done() and auctions_future.done()
      if not Utils.paused:
        Database.connect()
        Downloader._put_data_to_db(bazaar_data, Database.insert_bazaar)
        Downloader._put_data_to_db(auction_data, Database.insert_auctions)
        next(update_auction_prices)
        sleep(0.1)
      else:
        Database.disconnect()
        Utils.sleep_while(lambda: Utils.paused, 10)
      if (queue := len(bazaar_data + auction_data)) > 2:
        Utils.log(f'{queue} datas are waiting in queue', save=True)
