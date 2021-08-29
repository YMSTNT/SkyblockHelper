from time import sleep, time

import numpy
from scipy import stats

from data_utils import DataUtils
from database import Database
from utils import Utils


class PriceUpdater:

  @staticmethod
  def update_auction_prices():
    while not Utils.quitting:
      query = Database.select('SELECT last_updated, name FROM AuctionPrices WHERE priority > 0')
      query.sort(key=lambda k: k['last_updated'] or 0)
      for price_data in query:
        if not Utils.paused:
          Database.connect()
          PriceUpdater._update_auction_price_for_id(price_data['name'])
          sleep(0.5)
        else:
          Database.disconnect()

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
