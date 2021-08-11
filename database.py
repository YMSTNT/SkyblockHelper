import json
import sqlite3 as sl


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
    tables = ["BazaarBuy", 'BazaarSell']
    for table in tables:
      Database.execute(f"""
        CREATE TABLE {table} (
          id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
          time INTEGER,
          {item_ids_sql}
        );
      """)

  @staticmethod
  def execute(sql: str):
    with Database.connection:
      return Database.connection.execute(sql.replace(':', '__'))

  @staticmethod
  def insert_bazaar(bazaar: dict):
    buy_values = []
    sell_values = []
    for id in Database.item_ids:
      product = bazaar['products'][id]['quick_status']
      buy_values.append(str(product['buyPrice']))
      sell_values.append(str(product['sellPrice']))
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
