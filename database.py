import json
from time import time

import mariadb

from data_utils import DataUtils
from name_resolver import NameResolver
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
        'BazaarBuyPrices', 'BazaarBuyVolumes', 'BazaarBuyMovingCoins', 'BazaarBuyOrders',
        'BazaarSellPrices', 'BazaarSellVolumes', 'BazaarSellMovingCoins', 'BazaarSellOrders'
    ]
    Database.setup()
    # load seen auction ids
    query = Database.get('SELECT name FROM AuctionPrices')
    Database.auction_ids = [r[0] for r in query]

  @staticmethod
  def connect():
    if not Database.connection:
      Utils.log('Connecting to database...')
      with open('data/db_auth.json', 'r') as f:
        c = json.load(f)
        Database.connection = mariadb.connect(user=c['user'], password=c['password'], host=c['host'], port=c['port'], database=c['database'])
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
  def put(sql: str):
    with Database.connection.cursor() as cur:
      cur.execute(sql)
      Database.connection.commit()

  @staticmethod
  def get(sql: str):
    result = []
    with Database.connection.cursor() as cur:
      cur.execute(sql)
      result = cur.fetchall()
    return result

  @staticmethod
  def putmany(sql: str, data):
    with Database.connection.cursor() as cur:
      cur.executemany(sql, data)
      Database.connection.commit()

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
      query = Database.get(f'SELECT last_updated, {product} FROM {table}')
      data[table] = {'times': [], 'values': []}
      for row in query:
        data[table]['times'].append(row[0])
        data[table]['values'].append(row[1])
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
    query = Database.get(f'SELECT last_updated, price, count, bin FROM EndedAuctions WHERE name = "{product}"')
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

  @staticmethod
  def load_all_price():
    result = {'buy': {}, 'sell': {}}
    bazaar_sql = ','.join([f'`{id}`' for id in Database.bazaar_ids])
    query = Database.get(f'SELECT {bazaar_sql} FROM BazaarBuyPrices ORDER BY id DESC LIMIT 1')
    bazaar_buy_row = query[0]
    query = Database.get(f'SELECT {bazaar_sql} FROM BazaarSellPrices ORDER BY id DESC LIMIT 1')
    bazaar_sell_row = query[0]
    for i in range(len(Database.bazaar_ids)):
      result['buy'][Database.bazaar_ids[i]] = bazaar_buy_row[i]
      result['sell'][Database.bazaar_ids[i]] = bazaar_sell_row[i]
    query = Database.get('SELECT name, buy_price, sell_price FROM AuctionPrices')
    for row in query:
      result['buy'][row[0]] = row[1]
      result['sell'][row[0]] = row[2]
    Database.all_prices = result
