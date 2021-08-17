import json
import sqlite3 as sl
from time import time

from data_utils import DataUtils
from nbt_decoder import NbtDecoder
from utils import Utils


class Database:

  @staticmethod
  def init():
    Database.connection = None
    Database.connect()
    with open('data/bazaar_items.json') as f:
      Database.bazaar_ids = json.loads(f.readline())
    Database.bazaar_tables = [
        'BazaarBuyPrice', 'BazaarBuyVolume', 'BazaarBuyMovingWeek', 'BazaarBuyOrders',
        'BazaarSellPrice', 'BazaarSellVolume', 'BazaarSellMovingWeek', 'BazaarSellOrders'
    ]

  @staticmethod
  def connect():
    if not Database.connection:
      Utils.log('Connecting to database...')
      Database.connection = sl.connect('data/hypixel-skyblock.db')
      Utils.log('Connected to database', save=True)

  @staticmethod
  def disconnect():
    if Database.connection:
      Utils.log('Disconnecting from database...')
      Database.connection.commit()
      Database.connection.close()
      Database.connection = None
      Utils.log('Disconnected from database', save=True)

  @staticmethod
  def setup():
    item_ids_sql = [f'{id} REAL' for id in Database.bazaar_ids]
    item_ids_sql = ','.join(item_ids_sql)
    for table in Database.bazaar_tables:
      Database.try_execute(f"""
        CREATE TABLE {table} (
          id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
          time INTEGER,
          {item_ids_sql}
        );
      """)

    Database.try_execute(f"""
      CREATE TABLE EndedAuctions (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        time INTEGER,
        name TEXT,
        count INTEGER,
        price REAL,
        unit_price REAL,
        bin INTEGER,
        nbt BLOB
      );
    """)

  @staticmethod
  def execute(sql: str):
    with Database.connection:
      return Database.connection.execute(sql.replace(':', '__'))

  @staticmethod
  def executemany(sql: str, data):
    with Database.connection:
      return Database.connection.executemany(sql.replace(':', '__'), data)

  @staticmethod
  def try_execute(sql: str):
    try:
      Database.execute(sql)
    except:
      Utils.log('SQL FAILED, SKIPPING:', sql[0:50], '...')

  @staticmethod
  def insert_bazaar(bazaar: dict):
    data = {table: [] for table in Database.bazaar_tables}
    for id in Database.bazaar_ids:
      product = bazaar['products'][id]
      quick_status = product['quick_status']
      if product['buy_summary']:

        data['BazaarBuyPrice'].append(product['buy_summary'][0]['pricePerUnit'])
      else:
        data['BazaarBuyPrice'].append(-1)
      data['BazaarBuyVolume'].append(quick_status['buyVolume'])
      data['BazaarBuyMovingWeek'].append(quick_status['buyMovingWeek'])
      data['BazaarBuyOrders'].append(quick_status['buyOrders'])

      if product['sell_summary']:
        data['BazaarSellPrice'].append(product['sell_summary'][0]['pricePerUnit'])
      else:
        data['BazaarSellPrice'].append(-1)
      data['BazaarSellVolume'].append(quick_status['sellVolume'])
      data['BazaarSellMovingWeek'].append(quick_status['sellMovingWeek'])
      data['BazaarSellOrders'].append(quick_status['sellOrders'])

    item_ids_sql = ",".join(Database.bazaar_ids)
    for table_name, values in data.items():
      values_sql = ','.join([str(v) for v in values])
      Database.execute(f'INSERT INTO {table_name}(time, {item_ids_sql}) VALUES({bazaar["lastUpdated"]}, {values_sql})')

  @staticmethod
  def get_product_from_bazaar(product: str, complex=False):
    data = {}
    tables = Database.bazaar_tables if complex else ['BazaarBuyPrice', 'BazaarSellPrice']
    for table in tables:
      query = Database.execute(f'SELECT time, {product} FROM {table}')
      data[table] = {'times': [], 'values': []}
      for row in query:
        data[table]['times'].append(row[0])
        data[table]['values'].append(row[1])
    return data

  @staticmethod
  def insert_auctions(auctions):
    items = []
    for auction in auctions['auctions']:
      nbt_data = NbtDecoder.get_item_data_from_bytes(auction['item_bytes'])
      unit_price = auction['price'] / nbt_data['count']
      items.append((auction['timestamp'], nbt_data['name'], nbt_data['count'],
                    auction['price'], unit_price, auction['bin'], auction['item_bytes']))
    Database.executemany(
        'INSERT INTO EndedAuctions(time, name, count, price, unit_price, bin, nbt) VALUES(?, ?, ?, ?, ?, ?, ?)', items)

  @staticmethod
  def get_product_from_auction(product: str, complex=False):
    if not complex:
      data = {'times': [], 'values': []}
    else:
      data = {
          'AuctionPrice': {
              '1 hour': {'times': [], 'values': []},
              '6 hours': {'times': [], 'values': []},
              '1 day': {'times': [], 'values': []},
          },
          'AuctionVolume': {'times': [], 'values': []},
          'AuctionMovingCoins': {'times': [], 'values': []},
          'AuctionOrders': {'times': [], 'values': []},
      }
    query = Database.execute(f'SELECT time, price, count, bin FROM EndedAuctions WHERE name = "{product}"')
    query = [{'time': r[0], 'price': r[1], 'count': r[2], 'bin': r[3]} for r in query]
    for row in query:
      row['unit_price'] = row['price'] / row['count']
    average_price = DataUtils.get_average_price(query, DataUtils.DAY * 3)
    # filter out unusual prices
    filtered = list(filter(lambda r:
                           r['unit_price'] < average_price * 2 and
                           r['unit_price'] > average_price / 2,
                           query))
    # filter to only bin and stack count 1 items
    clean_prices = list(filter(lambda r: r['count'] == 1 and r['bin'] == 1, filtered))

    if not complex:
      smoothened = DataUtils.smooth_data(clean_prices, DataUtils.DAY, 'unit_price')
      for row in smoothened:
        data['times'].append(row['time'])
        data['values'].append(row['price'])
      return data
    else:
      smootheneds = {
          '1 hour': DataUtils.smooth_data(clean_prices, DataUtils.HOUR, 'unit_price'),
          '6 hours': DataUtils.smooth_data(clean_prices, DataUtils.HOUR * 6, 'unit_price'),
          '1 day': DataUtils.smooth_data(clean_prices, DataUtils.DAY, 'unit_price'),
      }
      for smoothened_time, smoothened in smootheneds.items():
        for row in smoothened:
          data['AuctionPrice'][smoothened_time]['times'].append(row['time'])
          data['AuctionPrice'][smoothened_time]['values'].append(row['unit_price'])

      # max 8 days old data
      eight_days_ago = int(time() * 1000) - DataUtils.DAY * 8
      filtered = list(filter(lambda r: r['time'] > eight_days_ago, filtered))

      # calculate daily moving coins, volume and orders
      filtered_week = filtered.copy()
      filtered_week.reverse()
      i = 0
      now = int(time() * 1000)
      for time_start in range(now, now - DataUtils.DAY * 7, -DataUtils.HOUR):
        sum_prices = []
        sum_volume = []
        sum_orders = 0
        j = i
        while j < len(filtered_week) and filtered_week[j]['time'] > time_start - DataUtils.DAY:
          if filtered_week[j]['time'] > time_start - DataUtils.HOUR:
            i = j
          sum_prices.append(filtered_week[j]['price'])
          sum_volume.append(filtered_week[j]['count'])
          sum_orders += 1
          j += 1
        sum_prices = sum(sum_prices)
        sum_volume = sum(sum_volume)
        data['AuctionMovingCoins']['times'].append(time_start)
        data['AuctionMovingCoins']['values'].append(sum_prices)
        data['AuctionVolume']['times'].append(time_start)
        data['AuctionVolume']['values'].append(sum_volume)
        data['AuctionOrders']['times'].append(time_start)
        data['AuctionOrders']['values'].append(sum_orders)
      data['AuctionMovingCoins']['times'].reverse()
      data['AuctionMovingCoins']['values'].reverse()
      data['AuctionVolume']['times'].reverse()
      data['AuctionVolume']['values'].reverse()
      data['AuctionOrders']['times'].reverse()
      data['AuctionOrders']['values'].reverse()

      return data
