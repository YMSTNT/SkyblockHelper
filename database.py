import json
import sqlite3 as sl

from nbt_decoder import NbtDecoder


class Database:

  @staticmethod
  def init():
    Database.connection = sl.connect('data/hypixel-skyblock.db')
    with open('data/bazaar_items.json') as file:
      Database.item_ids = json.loads(file.readline())
    Database.bazaar_tables = [
        'BazaarBuyPrice', 'BazaarBuyVolume', 'BazaarBuyMovingWeek', 'BazaarBuyOrders',
        'BazaarSellPrice', 'BazaarSellVolume', 'BazaarSellMovingWeek', 'BazaarSellOrders'
    ]

  @staticmethod
  def setup():
    item_ids_sql = [f'{id} REAL' for id in Database.item_ids]
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
      print('SQL FAILED, SKIPPING:', sql[0:50], '...')

  @staticmethod
  def insert_bazaar(bazaar: dict):
    data = {table: [] for table in Database.bazaar_tables}
    for id in Database.item_ids:
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

    item_ids_sql = ",".join(Database.item_ids)
    for table_name, values in data.items():
      values_sql = ','.join([str(v) for v in values])
      Database.execute(f'INSERT INTO {table_name}(time, {item_ids_sql}) VALUES({bazaar["lastUpdated"]}, {values_sql})')

  @staticmethod
  def get_bazaar_for_product(product: str):
    data = {}
    for table in Database.bazaar_tables:
      times = []
      values = []
      query = Database.execute(f'SELECT time, {product} FROM {table}')
      for row in query:
        times.append(row[0])
        values.append(row[1])
      data[table] = {'times': times, 'values': values}
    return data

  @staticmethod
  def insert_auctions(auctions):
    items = []
    for auction in auctions['auctions']:
      nbt_data = NbtDecoder.get_item_data_from_bytes(auction['item_bytes'])
      price_per_unit = auction['price'] / nbt_data['count']
      items.append((auction['timestamp'], nbt_data['name'], nbt_data['count'],
                    auction['price'], price_per_unit, auction['bin'], auction['item_bytes']))
    Database.executemany(
        'INSERT INTO EndedAuctions(time, name, count, price, unit_price, bin, nbt) VALUES(?, ?, ?, ?, ?, ?, ?)', items)
