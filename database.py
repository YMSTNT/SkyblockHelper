import sqlite3 as sl


class Database:

  @staticmethod
  def init():
    Database.connection = sl.connect('data/hypixel-skyblock.db')
    with open('data/item-ids.txt') as file:
      Database.item_ids = [id.strip() for id in file.readlines()]

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
      Database.connection.execute(sql.replace(':', '__'))

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