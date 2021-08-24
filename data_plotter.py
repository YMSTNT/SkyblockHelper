from time import time

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from database import Database
from name_resolver import NameResolver


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
  def show_bazaar(product: str, complex=False):
    product_data = Database.get_product_from_bazaar(NameResolver.to_id(product), complex)
    fig = plt.figure()
    fig.suptitle(NameResolver.to_name(product), fontsize=24)
    if complex:
      gs = gridspec.GridSpec(2, 2)
    else:
      gs = gridspec.GridSpec(1, 1)

    ax_price = plt.subplot(gs[0, 0])
    ax_price.plot(product_data['BazaarBuyPrices']['times'],
                  product_data['BazaarBuyPrices']['values'], 'r', label='Insta Buy = Sell Offer')
    ax_price.plot(product_data['BazaarSellPrices']['times'],
                  product_data['BazaarSellPrices']['values'], 'b', label='Insta Sell = Buy Order')
    ax_price.legend()
    ax_price.set_ylabel('Price', fontsize=18)
    ax_price.set_xlabel('Time ago', fontsize=18)
    ax_price.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
    ax_price.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
    fig.add_subplot(ax_price)
    plt.grid()

    if complex:
      ax_volume = plt.subplot(gs[0, 1])
      ax_volume.plot(product_data['BazaarBuyVolumes']['times'],
                     product_data['BazaarBuyVolumes']['values'], 'r', label='Insta Buy = Sell Offer')
      ax_volume.plot(product_data['BazaarSellVolumes']['times'],
                     product_data['BazaarSellVolumes']['values'], 'b', label='Insta Sell = Buy Order')
      ax_volume.legend()
      ax_volume.set_ylabel('Volume', fontsize=18)
      ax_volume.set_xlabel('Time ago', fontsize=18)
      ax_volume.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_volume.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_volume)
      plt.grid()

      ax_moving = plt.subplot(gs[1, 0])
      ax_moving.plot(product_data['BazaarBuyMovingCoins']['times'],
                     product_data['BazaarBuyMovingCoins']['values'], 'r', label='Insta Buy = Sell Offer')
      ax_moving.plot(product_data['BazaarBuyMovingCoins']['times'],
                     product_data['BazaarBuyMovingCoins']['values'], 'b', label='Insta Sell = Buy Order')
      ax_moving.legend()
      ax_moving.set_ylabel('Weekly Moving Coins', fontsize=18)
      ax_moving.set_xlabel('Time ago', fontsize=18)
      ax_moving.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_moving.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_moving)
      plt.grid()

      ax_orders = plt.subplot(gs[1, 1])
      ax_orders.plot(product_data['BazaarBuyOrders']['times'],
                     product_data['BazaarBuyOrders']['values'], 'r', label='Insta Buy = Sell Offer')
      ax_orders.plot(product_data['BazaarSellOrders']['times'],
                     product_data['BazaarSellOrders']['values'], 'b', label='Insta Sell = Buy Order')
      ax_orders.legend()
      ax_orders.set_ylabel('Orders', fontsize=18)
      ax_orders.set_xlabel('Time ago', fontsize=18)
      ax_orders.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_orders.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_orders)
      plt.grid()

    plt.show()

  @staticmethod
  def show_auction(product: str, complex=False):
    product_data = Database.get_product_from_auction(NameResolver.to_id(product), complex)
    fig = plt.figure()
    fig.suptitle(NameResolver.to_name(product), fontsize=24)
    if not complex:
      gs = gridspec.GridSpec(1, 1)
      ax_price = plt.subplot(gs[0, 0])
      ax_price.plot(product_data['times'], product_data['values'], 'r')
      ax_price.set_ylabel('Price', fontsize=18)
      ax_price.set_xlabel('Time ago', fontsize=18)
      ax_price.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_price.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_price)
      plt.grid()
    else:
      gs = gridspec.GridSpec(2, 2)

      ax_price = plt.subplot(gs[0, 0])
      ax_price.plot(product_data['AuctionPrice']['1 hour']['times'],
                    product_data['AuctionPrice']['1 hour']['values'],
                    'r', linewidth=1, label='1 hour')
      ax_price.plot(product_data['AuctionPrice']['6 hours']['times'],
                    product_data['AuctionPrice']['6 hours']['values'],
                    'g', linewidth=2, label='6 hours')
      ax_price.plot(product_data['AuctionPrice']['1 day']['times'],
                    product_data['AuctionPrice']['1 day']['values'],
                    'b', linewidth=3, label='1 day')
      ax_price.legend()
      ax_price.set_ylabel('Price', fontsize=18)
      ax_price.set_xlabel('Time ago', fontsize=18)
      ax_price.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_price.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_price)
      plt.grid()

      ax_moving = plt.subplot(gs[1, 0])
      ax_moving.plot(product_data['AuctionMovingCoins']['times'], product_data['AuctionMovingCoins']['values'], 'r')
      ax_moving.set_ylabel('Daily Moving Coins', fontsize=18)
      ax_moving.set_xlabel('Time ago', fontsize=18)
      ax_moving.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_moving.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_moving)
      plt.grid()

      ax_volume = plt.subplot(gs[0, 1])
      ax_volume.plot(product_data['AuctionVolume']['times'], product_data['AuctionVolume']['values'], 'r')
      ax_volume.set_ylabel('Daily Items Sold', fontsize=18)
      ax_volume.set_xlabel('Time ago', fontsize=18)
      ax_volume.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_volume.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_volume)
      plt.grid()

      ax_orders = plt.subplot(gs[1, 1])
      ax_orders.plot(product_data['AuctionOrders']['times'], product_data['AuctionOrders']['values'], 'r')
      ax_orders.set_ylabel('Daily Auctions Filled', fontsize=18)
      ax_orders.set_xlabel('Time ago', fontsize=18)
      ax_orders.yaxis.set_major_formatter(FuncFormatter(DataPlotter._format_coins))
      ax_orders.xaxis.set_major_formatter(FuncFormatter(DataPlotter._format_date))
      fig.add_subplot(ax_orders)
      plt.grid()

    plt.show()
