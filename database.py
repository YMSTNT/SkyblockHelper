import json
import sqlite3 as sl

from nbt_decoder import NbtDecoder


class Database:

  @staticmethod
  def init():
    Database.connection = sl.connect('data/hypixel-skyblock.db')
    with open('data/bazaar_items.json') as file:
      Database.item_ids = json.loads(file.readline())

  @staticmethod
  def setup():
    item_ids_sql = [f'{id} REAL' for id in Database.item_ids]
    item_ids_sql = ','.join(item_ids_sql)
    for table in ["BazaarBuy", 'BazaarSell']:
      Database.execute(f"""
        CREATE TABLE {table} (
          id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
          time INTEGER,
          {item_ids_sql}
        );
      """)

    Database.execute(f"""
      CREATE TABLE EndedAuctions (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        time INTEGER,
        name TEXT,
        price INTEGER,
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
  def insert_bazaar(bazaar: dict):
    buy_values = []
    sell_values = []
    for id in Database.item_ids:
      product = bazaar['products'][id]
      if product['buy_summary']:
        buy_values.append(str(product['buy_summary'][0]['pricePerUnit']))
      else:
        buy_values.append(str(-1))
      if product['sell_summary']:
        sell_values.append(str(product['sell_summary'][0]['pricePerUnit']))
      else:
        sell_values.append(str(-1))
    item_ids_sql = ",".join(Database.item_ids)
    buy_values_sql = ','.join(buy_values)
    sell_values_sql = ','.join(sell_values)
    Database.execute(
        f'INSERT INTO BazaarBuy(time, {item_ids_sql}) VALUES({bazaar["lastUpdated"]}, {buy_values_sql})')
    Database.execute(
        f'INSERT INTO BazaarSell(time, {item_ids_sql}) VALUES({bazaar["lastUpdated"]}, {sell_values_sql})')

  @staticmethod
  def get_bazaar_prices_for_product(product: str):
    result = {
        'buy': {'times': [], 'prices': []},
        'sell': {'times': [], 'prices': []}
    }
    buy_data = Database.execute(f'SELECT time, {product} FROM BazaarBuy')
    sell_data = Database.execute(f'SELECT time, {product} FROM BazaarSell')
    for row in buy_data:
      result['buy']['times'].append(row[0])
      result['buy']['prices'].append(row[1])
    for row in sell_data:
      result['sell']['times'].append(row[0])
      result['sell']['prices'].append(row[1])
    return result

  @staticmethod
  def insert_auctions(auctions):
    items = []
    for auction in auctions['auctions']:
      item_name = NbtDecoder.get_item_id_from_bytes(auction['item_bytes'])
      items.append((auction['timestamp'], item_name,
                   auction['price'], auction['bin'], auction['item_bytes']))
    Database.executemany(
        'INSERT INTO EndedAuctions(time, name, price, bin, nbt) VALUES(?, ?, ?, ?, ?)', items)
