from time import time
from events import Events

class AssistantApi:
  def main(endpoint):
    if endpoint == 'events':
      return AssistantApi.events()
    return {'status': 404, 'data': ''}

  def events():
    placeholder = {
      'success': True,
      'events': [
        {
          'type': 'daily',
          'category': 'jacob',
          'name': 'Jacob\'s Farming Contest',
          'start': int(time() * 1000) + 1000 * 60 * 5,
          'end': int(time() * 1000) + 1000 * 60 * 25,
        },
        {
          'type': 'weekly',
          'category': 'election_over',
          'name': 'Election Over',
          'start': int(time() * 1000) + 1000 * 60 * 60 * 24 * 3,
          'end': int(time() * 1000) + 1000 * 60 * 60 * 24 * 3,
        },
        {
          'type': 'common_mayor',
          'category': 'mining_fiesta',
          'name': 'Mining Fiesta',
          'start': int(time() * 1000) + 1000 * 60 * 60 * 24 * 3,
          'end': int(time() * 1000) + 1000 * 60 * 60 * 24 * 3,
        },
        {
          'type': 'special_mayor',
          'category': 'technoblade',
          'name': '<insert techno\'s perks\' name here>',
          'start': int(time() * 1000) + 1000 * 60 * 60 * 24 * 3,
          'end': int(time() * 1000) + 1000 * 60 * 60 * 24 * 3,
        }
      ]
    }
    return {'status': 200, 'data': Events.get()}

