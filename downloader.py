
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
    query = Database.select(f'SELECT price, count FROM EndedAuctions WHERE name = "{id}" AND last_updated > {three_days_ago}')
    prices = [r['price'] / r['count'] for r in query]
    if prices:
      mode = int(stats.mode(prices)[0])
      median = numpy.median(prices)
      average_price = (mode + median) / 2
    else:
      average_price = -1
    # insert average to db
    Database.put(f'UPDATE AuctionPrices SET buy_price = {average_price}, sell_price = {average_price}, last_updated = {now} WHERE name = "{id}"')
    Utils.log(f'[ah] Updated price for {id}', now)

  @staticmethod
  def _update_auction_prices():
    while True:
      query = Database.select('SELECT last_updated, name FROM AuctionPrices WHERE priority > 0')
      query.sort(key=lambda k: k['last_updated'] or 0)
      if not query:
        yield
      for price_data in query:
        yield Downloader._update_auction_price_for_id(price_data['name'])

  @staticmethod
  def download_and_save_data():
    executor = ThreadPoolExecutor()
    bazaar_data, auction_data = Queue(), Queue()
    bazaar_task = executor.submit(SkyblockApi.get_new_bazaar, bazaar_data)
    auctions_task = executor.submit(SkyblockApi.get_new_ended_auctions, auction_data)
    executor.submit(Downloader._catch_input)
    update_auction_prices = Downloader._update_auction_prices()
    should_quit = False
    while not should_quit:
      should_quit = Utils.quitting and not bazaar_data and not auction_data and bazaar_task.done() and auctions_task.done()
      if not Utils.paused:
        Database.connect()
        next(update_auction_prices)
        Downloader._put_data_to_db(bazaar_data, Database.insert_bazaar)
        Downloader._put_data_to_db(auction_data, Database.insert_auctions)
        sleep(0.1)
      else:
        Database.disconnect()
        Utils.sleep_while(lambda: Utils.paused, 10)
      if (len_datas := len(bazaar_data) + len(auction_data)) > 2:
        Utils.log(f'{len_datas} datas are waiting in queue', save=True)
