
from argparse import ArgumentParser
from concurrent.futures.thread import ThreadPoolExecutor

from dotenv import load_dotenv

from database import Database
from database_cleaner import DatabaseCleaner
from downloader import Downloader
from forge_optimizer import ForgeOptimizer
from name_resolver import NameResolver
from price_updater import PriceUpdater
from skyblock_api import SkyblockApi
from utils import Utils
from assistant_api import AssistantApi
import json


class Main:
  @staticmethod
  def init():
    args = Main._parse_args()
    Utils.debug = args.debug
    Utils.silent = args.silent
    load_dotenv('data/.env')
    SkyblockApi.init()
    Database.init()
    NameResolver.init()
    ForgeOptimizer.init()

    executor = ThreadPoolExecutor()
    if args.download or args.update:
      executor.submit(Main._catch_input)
    if args.main:
      Main.main()
    if args.download:
      Downloader.download_and_save_data(executor)
    if args.update:
      executor.submit(PriceUpdater.update_auction_prices)
    if args.api:
      response = AssistantApi.main(args.api)
      print(json.dumps(response))
    if args.clean:
      DatabaseCleaner.clean()

  @staticmethod
  def _parse_args():
    parser = ArgumentParser()
    parser.add_argument('-m', '--main', action='store_true',
                        help='main function')
    parser.add_argument('-d', '--download', action='store_true',
                        help='download skyblock data from Hypixel and store it in the database')
    parser.add_argument('-u', '--update', action='store_true',
                        help='update current auction house buy and sell prices')
    parser.add_argument('-a', '--api', nargs='?',
                        help='start a flask webserver listening for API requests')
    parser.add_argument('-b', '--debug', action='store_true',
                        help='use the debug database')
    parser.add_argument('-c', '--clean', action='store_true',
                        help='clean old records in the database')
    parser.add_argument('-s', '--silent', action='store_true',
                        help='hide logs in the output')
    args = parser.parse_args()
    if not any(vars(args).values()):
      parser.error('No arguments provided.')
    return args

  @staticmethod
  def main():
    from data_plotter import DataPlotter
    DataPlotter.show_bazaar('BOOSTER_COOKIE', complex=True)
    DataPlotter.show_auction('GOD_POTION_2', complex=True)
    ForgeOptimizer.optimize()

  @staticmethod
  def _catch_input():
    while not Utils.quitting:
      inp = input()
      if inp == 'q':
        Utils.quitting = True
        Utils.log('Quitting...')
      elif inp == 'p':
        Utils.paused = not Utils.paused
        if Utils.paused:
          Utils.log('Pausing database access...')
        else:
          Utils.log('Resumed database access')


if __name__ == '__main__':
  Main.init()
