from data_utils import DataUtils

class SkyblockTime:
  SKYBLOCK_START = 1559829301000

  MINUTE = 1000 * (50 / 60)
  HOUR = int(MINUTE * 60)
  DAY = HOUR * 24
  MONTH = DAY * 31
  YEAR = MONTH * 12

  @staticmethod
  def skyblock_time_to_epoch(sb_time):
    sb_time['years'] = sb_time.get('years') or 0
    sb_time['months'] = sb_time['years'] * 12 + (sb_time.get('months') or 0)
    sb_time['days'] = (sb_time['months'] - 1) * 31 + (sb_time.get('days') or 0)
    sb_time['hours'] = (sb_time['days'] - 1) * 24 + (sb_time.get('hours') or 0)
    sb_time['minutes'] = sb_time['hours'] * 60 + (sb_time.get('minutes') or 0)
    sb_time['ms'] = sb_time['minutes'] * (50 / 60) * 1000 + (sb_time.get('ms') or 0)
    return SkyblockTime.SKYBLOCK_START + int(sb_time['ms'])

  @staticmethod
  def epoch_to_skyblock_time(epoch):
    total_ms = epoch - SkyblockTime.SKYBLOCK_START
    total_minutes = total_ms / 1000 / (50 / 60)
    total_hours = total_minutes / 60
    total_days = total_hours / 24
    total_months = total_days / 31
    total_years = total_months / 12
    return {
      'years': int(total_years),
      'months': int(total_months % 12) + 1,
      'days': int(total_days % 31) + 1,
      'hours': int(total_hours % 24),
      'minutes': int(total_minutes % 60),
      'ms': int((total_ms + DataUtils.EPSILON) % (1000 * (50 / 60)))
    }
