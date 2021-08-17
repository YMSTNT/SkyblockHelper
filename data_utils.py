from time import time

import numpy
from scipy import stats


class DataUtils:

  HOUR = 1000 * 60 * 60
  DAY = HOUR * 24

  @staticmethod
  def smooth_data(data, smooth_time: int, data_index: str):
    data = data.copy()
    data.reverse()
    smoothened = []
    i = 0
    while i < len(data):
      time_ago = data[i]['time'] - smooth_time
      average = []
      j = i
      while j < len(data) and data[j]['time'] > time_ago:
        average.append(data[j][data_index])
        j += 1
      average_final = sum(average) / len(average)
      smoothened.append({'time': data[i]['time'], data_index: average_final})
      i += int(len(average) / 5 + 1)
    smoothened.reverse()
    return smoothened

  @staticmethod
  def get_average_price(prices, first_time_ago):
    first_average_time = int(time() * 1000) - first_time_ago
    average_price = filter(lambda r: r['time'] > first_average_time, prices)
    average_price = list(map(lambda r: r['price'], average_price))
    mode = int(stats.mode(average_price)[0])
    median = numpy.median(average_price)
    average_price = (mode + median) / 2
    return average_price
