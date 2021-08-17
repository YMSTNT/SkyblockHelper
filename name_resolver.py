import json
import os
import re


class NameResolver:

  @staticmethod
  def init():
    if not os.path.exists('data/item_translation.json'):
      NameResolver.cache()
    with open(f'data/item_translation.json', 'r') as f:
      data = json.load(f)
      NameResolver._id_to_name = data['id_to_name']
      NameResolver._name_to_id = data['name_to_id']
      NameResolver.ids = data['id_to_name'].keys()
      NameResolver.names = data['name_to_id'].keys()

  @staticmethod
  def cache():
    result = {'id_to_name': {}, 'name_to_id': {}}
    for id in os.listdir('data/NotEnoughUpdates-REPO/items'):
      with open(f'data/NotEnoughUpdates-REPO/items/{id}', 'r') as f:
        name = json.load(f)['displayname']
        # remove colors and formatting
        name = re.sub('§.', '', name)
        # remove pet level
        name = name.replace('[Lvl {LVL}] ', '')
        id = id.replace('.json', '')
        result['id_to_name'][id.lower()] = name
        result['name_to_id'][name.lower()] = id
    with open(f'data/item_translation.json', 'w') as f:
      json.dump(result, f)

  @staticmethod
  def to_name(id: str):
    id = id.lower()
    if id in NameResolver.ids:
      return NameResolver._id_to_name[id]
    if (name := id) in NameResolver.names:
      return NameResolver._id_to_name[NameResolver._name_to_id[name].lower()]
    return None

  @staticmethod
  def to_id(name: str):
    name = name.lower()
    if name in NameResolver.names:
      return NameResolver._name_to_id[name]
    if (id := name) in NameResolver.ids:
      return NameResolver._name_to_id[NameResolver._id_to_name[id].lower()]
    names = [key for key in NameResolver.names if name in key]
    if len(names) == 1:
      return names[0]
    return None
