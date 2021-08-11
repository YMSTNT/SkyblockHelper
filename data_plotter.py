from time import time

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from database import Database


class DataPlotter:
  
  @staticmethod
  def _format_coins(x: int, pos):
    if abs(x) < 1000:
      return x
    elif abs(x) < 1000 ** 2:
      return f'{x / 1000}k'
    elif abs(x) < 1000 ** 3:
      return f'{x / 1000 ** 2}m'
    else:
      return f'{x / 1000 ** 3}b'

  @staticmethod
  def _format_date(y: float, pos):
    sec = time() - y / 1000
    if abs(sec) < 60:
      return f'{round(sec, 1)}s'
    elif abs(sec) < 60 * 60:
      return f'{round(sec / 60, 1)}m'
    elif abs(sec) < 60 * 60 * 24:
      return f'{round(sec / 60 / 60, 1)}h'
    elif abs(sec) < 60 * 60 * 24 * 30:
      return f'{round(sec / 60 / 60 / 24, 1)}d'
    elif abs(sec) < 60 * 60 * 24 * 30 * 12:
      return f'{round(sec / 60 / 60 / 24 / 30, 1)}mo'
    else:
      return f'{round(sec / 60 / 60 / 24 / 30 / 12, 1)}y'
  
  @staticmethod
  def show_bazaar(product_id: str):
    product_data = Database.get_bazaar_prices_for_product(product_id)
    fig, ax = plt.subplots()
    fig.suptitle(product_id, fontsize=24)
    ax.legend()
    ax.plot(product_data['buy']['times'],
            product_data['buy']['prices'], 'r', label='Insta Buy = Sell Offer')
    ax.legend()
    ax.plot(product_data['sell']['times'],
            product_data['sell']['prices'], 'b', label='Insta Sell = Buy Order')
    ax.legend()
    ax.set_ylabel('coins', fontsize=18)
    ax.set_xlabel('time ago', fontsize=18)
    ax.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
    ax.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
    ax.grid()
    plt.show()
