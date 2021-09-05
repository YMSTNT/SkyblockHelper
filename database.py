import json
import os
import re
from time import sleep, time

import mariadb

from data_utils import DataUtils
from name_resolver import NameResolver
from nbt_decoder import NbtDecoder
from utils import Utils


class Database:

  @staticmethod
  def init():
    Database._locked = False
    Database.connection = None
    Database.connect()
    with open('data/bazaar_items.json') as f:
      Database.bazaar_ids = json.load(f)
    Database.bazaar_tables = [
        'BazaarBuyPrices', 'BazaarBuyVolumes', 'BazaarBuyMovingCoins', 'BazaarBuyOrders',
        'BazaarSellPrices', 'BazaarSellVolumes', 'BazaarSellMovingCoins', 'BazaarSellOrders'
    ]
    Database.setup()
    # load seen auction ids
    query = Database.select('SELECT name FROM AuctionPrices')
    Database.auction_ids = [r['name'] for r in query]

  @staticmethod
  def connect():
    if not Database.connection:
      Utils.log('Connecting to database...')
      Database.connection = mariadb.connect(
          host=os.getenv('DB_HOST'), port=int(os.getenv('DB_PORT')),
          user=os.getenv('DB_USER'), password=os.getenv('DB_PASS'),
          database=os.getenv('DB_NAME_DEBUG' if Utils.debug else 'DB_NAME_PROD'))
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
    bazaar_ids_sql = [f'`{id}` DEC(20, 1)' for id in Database.bazaar_ids]
    bazaar_ids_sql = ','.join(bazaar_ids_sql)
    for table in Database.bazaar_tables:
      Database.put(f"""
        CREATE TABLE IF NOT EXISTS {table} (
          id INT PRIMARY KEY AUTO_INCREMENT,
          last_updated BIGINT,
          {bazaar_ids_sql}
        );
      """)

    Database.put(f"""
      CREATE TABLE IF NOT EXISTS EndedAuctions (
        id INT PRIMARY KEY AUTO_INCREMENT,
        last_updated BIGINT,
        name VARCHAR(100),
        count TINYINT,
        price INT,
        bin BOOLEAN,
        nbt BLOB
      );
    """)

    Database.put(f"""
      CREATE TABLE IF NOT EXISTS AuctionPrices (
        id INT PRIMARY KEY AUTO_INCREMENT,
        last_updated BIGINT,
        name VARCHAR(100),
        buy_price INT,
        sell_price INT,
        priority INT
      );
    """)

  @staticmethod
  def _lock():
    while Database._locked:
      sleep(0.1)
    Database._locked = True

  @staticmethod
  def _release():
    Database._locked = False

  @staticmethod
  def _retry(action):
    while True:
      try:
        action()
      except mariadb.OperationalError:
        Utils.log('DB FAILED, RETRYING')
      finally:
        return

  @staticmethod
  def execute(sql: str):
    Database._lock()
    with Database.connection.cursor() as cur:
      Database._retry(lambda: cur.execute(sql))
    Database._release()

  @staticmethod
  def put(sql: str):
    Database._lock()
    with Database.connection.cursor() as cur:
      Database._retry(lambda: cur.execute(sql))
      Database.connection.commit()
    Database._release()

  @staticmethod
  def get(sql: str):
    Database._lock()
    with Database.connection.cursor() as cur:
      Database._retry(lambda: cur.execute(sql))
      result = cur.fetchall()
    Database._release()
    return result

  @staticmethod
  def select(sql: str):
    result = []
    data = Database.get(sql)
    columns = re.search('SELECT (.*) FROM', sql).group(1)
    columns = columns.split(',')
    columns = [c.strip(' `') for c in columns]
    for row in data:
      new_row = {k: v for k, v in zip(columns, row)}
      result.append(new_row)
    return result

  @staticmethod
  def putmany(sql: str, data):
    Database._lock()
    with Database.connection.cursor() as cur:
      Database._retry(lambda: cur.executemany(sql, data))
      Database.connection.commit()
    Database._release()

  @staticmethod
  def insert_bazaar(bazaar: dict):
    data = {table: [] for table in Database.bazaar_tables}
    for id in Database.bazaar_ids:
      product = bazaar['products'][id]
      quick_status = product['quick_status']
      if product['buy_summary']:

        data['BazaarBuyPrices'].append(product['buy_summary'][0]['pricePerUnit'])
      else:
        data['BazaarBuyPrices'].append(-1)
      data['BazaarBuyVolumes'].append(quick_status['buyVolume'])
      data['BazaarBuyMovingCoins'].append(quick_status['buyMovingWeek'])
      data['BazaarBuyOrders'].append(quick_status['buyOrders'])

      if product['sell_summary']:
        data['BazaarSellPrices'].append(product['sell_summary'][0]['pricePerUnit'])
      else:
        data['BazaarSellPrices'].append(-1)
      data['BazaarSellVolumes'].append(quick_status['sellVolume'])
      data['BazaarSellMovingCoins'].append(quick_status['sellMovingWeek'])
      data['BazaarSellOrders'].append(quick_status['sellOrders'])

    item_ids_sql = ",".join([f'`{id}`' for id in Database.bazaar_ids])
    for table_name, values in data.items():
      values_sql = ','.join([str(v) for v in values])
      Database.put(f'INSERT INTO {table_name}(last_updated, {item_ids_sql}) VALUES({bazaar["lastUpdated"]}, {values_sql})')

  @staticmethod
  def get_product_from_bazaar(product: str, complex=False):
    product = NameResolver.to_id(product)
    data = {}
    tables = Database.bazaar_tables if complex else ['BazaarBuyPrices', 'BazaarSellPrices']
    for table in tables:
      query = Database.select(f'SELECT last_updated, {product} FROM {table}')
      data[table] = {'times': [], 'values': []}
      for row in query:
        data[table]['times'].append(row['last_updated'])
        data[table]['values'].append(row[product])
    return data

  @staticmethod
  def insert_auctions(auctions):
    # get items from auction data
    items = []
    for auction in auctions['auctions']:
      nbt_data = NbtDecoder.get_item_data_from_bytes(auction['item_bytes'])
      if nbt_data['count'] < 1:
        continue
      items.append((auction['timestamp'], nbt_data['name'], nbt_data['count'],
                    auction['price'], auction['bin'], auction['item_bytes']))
    # insert items to EndedAuctions
    Database.putmany(
        'INSERT INTO EndedAuctions(last_updated, name, count, price, bin, nbt) VALUES(?, ?, ?, ?, ?, ?)', items)
    # update seen auction item ids
    item_ids = set([r[1] for r in items])
    new_item_ids = [id for id in item_ids if id not in Database.auction_ids]
    for new_id in new_item_ids:
      Database.put(f'INSERT INTO AuctionPrices(name, priority) VALUES("{new_id}", 1)')
      Database.auction_ids.append(new_id)
      Utils.log(f'New auction item found: {new_id}', save=True)

  @staticmethod
  def get_product_from_auction(product: str, complex=False):
    product = NameResolver.to_id(product)
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
    query = Database.select(f'SELECT last_updated, price, count, bin FROM EndedAuctions WHERE name = "{product}"')
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
        data['times'].append(row['last_updated'])
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
          data['AuctionPrice'][smoothened_time]['times'].append(row['last_updated'])
          data['AuctionPrice'][smoothened_time]['values'].append(row['unit_price'])

      # max 8 days old data
      eight_days_ago = int(time() * 1000) - DataUtils.DAY * 8
      filtered = list(filter(lambda r: r['last_updated'] > eight_days_ago, filtered))

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
        while j < len(filtered_week) and filtered_week[j]['last_updated'] > time_start - DataUtils.DAY:
          if filtered_week[j]['last_updated'] > time_start - DataUtils.HOUR:
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

  @staticmethod
  def load_all_price():
    result = {'buy': {}, 'sell': {}}
    bazaar_sql = ','.join([f'`{id}`' for id in Database.bazaar_ids])
    query = Database.select(f'SELECT {bazaar_sql} FROM BazaarBuyPrices ORDER BY id DESC LIMIT 1')
    bazaar_buy_row = query[0]
    query = Database.select(f'SELECT {bazaar_sql} FROM BazaarSellPrices ORDER BY id DESC LIMIT 1')
    bazaar_sell_row = query[0]
    for id in Database.bazaar_ids:
      result['buy'][id] = bazaar_buy_row[id]
      result['sell'][id] = bazaar_sell_row[id]
    query = Database.select('SELECT name, buy_price, sell_price FROM AuctionPrices')
    for row in query:
      result['buy'][row['name']] = row['buy_price']
      result['sell'][row['name']] = row['sell_price']
    Database.all_prices = result
