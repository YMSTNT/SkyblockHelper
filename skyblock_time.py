class SkyblockTime:
  skyblock_start = 1559790901000

  @staticmethod
  def skyblock_time_to_epoch(sb_time):
    sb_time['months'] = sb_time['years'] * 12 + sb_time['months']
    sb_time['days'] = sb_time['months'] * 31 + sb_time['days']
    sb_time['hours'] = sb_time['days'] * 24 + sb_time['hours']
    sb_time['minutes'] = sb_time['hours'] * 60 + sb_time['minutes']
    sb_time['ms'] = sb_time['minutes'] * (50 / 60) * 1000 + sb_time['ms']
    return SkyblockTime.skyblock_start + int(sb_time['ms'])

  @staticmethod
  def epoch_to_skyblock_time(epoch):
    total_ms = epoch - SkyblockTime.skyblock_start
    total_minutes = total_ms / 1000 / (50 / 60)
    total_hours = total_minutes / 60
    total_days = total_hours / 24
    total_months = total_days / 31
    total_years = total_months / 12
    return {
      'years': int(total_years),
      'months': int(total_months % 12),
      'days': int(total_days % 31),
      'hours': int(total_hours % 24),
      'minutes': int(total_minutes % 60),
      'ms': int(total_ms % (1000 * (50 / 60)))
    }
