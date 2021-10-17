from skyblock_time import SkyblockTime
from time import time
from data_utils import DataUtils

class Events:
  @staticmethod
  def get():
    events = []
    events += Events._generate_events(type='other', id='zoo', name='Travelling Zoo',
      length=SkyblockTime.DAY * 3, frequency=SkyblockTime.YEAR,offset={'months': 1, 'days': 1})
    return events

  @staticmethod
  def _generate_events(type, id, name, length, frequency, offset):
    offset = SkyblockTime.skyblock_time_to_epoch(offset) - SkyblockTime.SKYBLOCK_START
    early_length = length - int(SkyblockTime.MINUTE) # end event at 23.59 instead of next day
    events = []
    first_time = Events._get_first_epoch()
    last_time = Events._get_last_epoch()
    for start in range(SkyblockTime.SKYBLOCK_START + offset, last_time, frequency):
      if start > first_time:
        event = {
          'type': type,
          'id': id,
          'name': name,
          'start': start,
          'end': start + early_length,
          # 'start_human': SkyblockTime.epoch_to_skyblock_time(start),
          # 'end_human': SkyblockTime.epoch_to_skyblock_time(start + early_length),
          'length': length
        }
        events.append(event)
    return events

  @staticmethod
  def _get_first_epoch():
    return int(time() * 1000) - DataUtils.DAY

  @staticmethod
  def _get_last_epoch():
    return int(time() * 1000) + DataUtils.DAY * 7
