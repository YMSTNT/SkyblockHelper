import json
import os
import re

from database import Database
from name_resolver import NameResolver


class ForgeOptimizer:

  @staticmethod
  def init():
    if not os.path.exists('data/forge_details.json'):
      ForgeOptimizer.cache()
    with open('data/forge_details.json') as f:
      ForgeOptimizer.details = json.load(f)

  @staticmethod
  def cache():
    with open('data/forge_items.json', 'r') as f:
      forge_items = json.load(f)
    result = {}
    for item in forge_items:
      with open(f'data/NotEnoughUpdates-REPO/items/{item}.json', 'r') as f:
        data = json.load(f)
      result[item] = {'ingredients': {}}
      found_items_required = False
      for lore in data['lore']:
        lore = re.sub('§.', '', lore)
        if not lore:
          continue
        if 'Duration' in lore:
          sec = re.search('(\d+)s', lore) or 0
          if sec:
            sec = int(sec.group(0).replace('s', ''))
          min = re.search('(\d+)m', lore) or 0
          if min:
            min = int(min.group(0).replace('m', ''))
          hour = re.search('(\d+)h', lore) or 0
          if hour:
            hour = int(hour.group(0).replace('h', ''))
          day = re.search('(\d+)d', lore) or 0
          if day:
            day = int(day.group(0).replace('d', ''))
          duration = (((day * 24 + hour) * 60 + min) * 60 + sec) * 1000
          result[item]['duration'] = duration
        elif found_items_required:
          if 'Coins' in lore:
            coins = int(lore.replace('Coins', '').replace(',', '').strip())
            result[item]['ingredients']['COINS'] = coins
          else:
            ingredient_count = re.search('x\d+', lore).group(0)
            ingredient_name = lore.replace(ingredient_count, '').strip()
            ingredient_name = ingredient_name.replace('Model ', '')
            ingredient_id = NameResolver.to_id(ingredient_name)
            ingredient_count = int(ingredient_count.replace('x', ''))
            result[item]['ingredients'][ingredient_id] = ingredient_count
        elif 'Items Required' in lore:
          found_items_required = True
    with open('data/forge_details.json', 'w') as f:
      json.dump(result, f)

  @staticmethod
  def optimize():
    Database.load_all_price()
    profits = []
    for item, details in ForgeOptimizer.details.items():
      try:
        product_price = Database.all_prices['sell'][item]
        ingredient_price = 0
        for ingredient, ingredient_count in details['ingredients'].items():
          ingredient_price += Database.all_prices['buy'][ingredient] * ingredient_count
        profit = int(product_price - ingredient_price) / 1000
        product_name = NameResolver.to_name(item)
        forge_hours = round(details['duration'] / 1000 / 60 / 60, 2)
        profits.append({'profit': f'{profit}k', 'name': product_name, 'hours': forge_hours})
      except (KeyError, TypeError):
        print(f'Skipping {item} due to lack of price info')
    profits.sort(key=lambda p: p['profit'], reverse=True)
    for p in profits:
      print(f'{p["profit"]} coins per item profit for {p["name"]}, it takes {p["hours"]} hours to forge')
