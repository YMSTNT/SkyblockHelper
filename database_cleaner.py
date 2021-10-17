from time import time

from data_utils import DataUtils
from database import Database
from utils import Utils
import random


class DatabaseCleaner:
  def clean():
    DatabaseCleaner.clean_bazaar()
    DatabaseCleaner.clean_auctions()

  def clean_bazaar():
    Utils.log('[bz] Cleaning bazaar tables...', save=True)
    a_day_ago = int(time() * 1000) - DataUtils.DAY
    for table in Database.bazaar_tables:
      query = Database.select(f'SELECT id, last_updated FROM {table} WHERE last_updated < {a_day_ago} ORDER BY last_updated DESC')
      ids = []
      last_time = int(time() * 1000)
      for row in query:
        time_difference_from_now = int(time() * 1000) - row['last_updated']
        time_difference_from_last_row = last_time - row['last_updated']
        if (time_difference_from_now > DataUtils.DAY and time_difference_from_last_row < DataUtils.MINUTE) or \
           (time_difference_from_now > DataUtils.DAY * 7 and time_difference_from_last_row < DataUtils.MINUTE * 15) or \
           (time_difference_from_now > DataUtils.DAY * 30 and time_difference_from_last_row < DataUtils.HOUR) or \
           (time_difference_from_now > DataUtils.DAY * 365 and time_difference_from_last_row < DataUtils.DAY):
          ids.append((row['id'],))
        else:
          last_time = row['last_updated']
      Utils.log(f'[bz] Deleting {len(ids)} of {len(query)} rows from {table}...', save=True)
      if ids:
        rows_per_query = 100_000
        for i in range(0, len(ids), rows_per_query):
          sub_ids = ids[i: i + rows_per_query]
          Database.putmany(f'DELETE FROM {table} WHERE id = ?', sub_ids)
          Utils.log(f'[bz] Deleted {len(sub_ids)} rows from {table}', save=True)
      Utils.log(f'[bz] Optimizing table {table}...', save=True)
      Database.execute(f'OPTIMIZE TABLE {table}')
      Utils.log(f'[bz] Optimized table {table}', save=True)

  def clean_auctions():
    Utils.log('[ah] Cleaning EndedAuctions table...', save=True)
    a_week_ago = int(time() * 1000) - DataUtils.DAY * 7
    Utils.log('[ah] Counting auctions...', save=True)
    query = Database.select(f'SELECT name FROM EndedAuctions WHERE last_updated < {a_week_ago} GROUP BY name ORDER BY COUNT(*) DESC')
    ids = []
    for row in query:
        ids.append(row['name'])
    Utils.log('[ah] Counted auctions', save=True)
    for item in ids:
      query = Database.select(f'SELECT id, last_updated FROM EndedAuctions WHERE last_updated < {a_week_ago} AND name = "{item}" ORDER BY last_updated DESC')
      ids = []
      last_time = int(time() * 1000)
      for row in query:
        time_difference_from_now = int(time() * 1000) - row['last_updated']
        time_difference_from_last_row = last_time - row['last_updated']
        if (time_difference_from_now > DataUtils.DAY * 7 and time_difference_from_last_row < DataUtils.MINUTE * 15) or \
           (time_difference_from_now > DataUtils.DAY * 30 and time_difference_from_last_row < DataUtils.HOUR) or \
           (time_difference_from_now > DataUtils.DAY * 365 and time_difference_from_last_row < DataUtils.DAY):
          ids.append((row['id'],))
        else:
          last_time = row['last_updated']
      Utils.log(f'[ah] Deleting {len(ids)} of {len(query)} rows from EndedAuctions WHERE name = {item}...', save=True)
      if ids:
        rows_per_query = 10_000
        for i in range(0, len(ids), rows_per_query):
          sub_ids = ids[i: i + rows_per_query]
          Database.putmany('DELETE FROM EndedAuctions WHERE id = ?', sub_ids)
          Utils.log(f'[ah] Deleted {len(sub_ids)} rows from EndedAuctions WHERE name = {item}', save=True)
    Utils.log('[ah] Optimizing table EndedAuctions...', save=True)
    Database.execute(f'OPTIMIZE TABLE EndedAuctions')
    Utils.log('[ah] Optimized table EndedAuctions', save=True)
