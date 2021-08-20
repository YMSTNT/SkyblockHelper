
import sys

from data_plotter import DataPlotter
from database import Database
from downloader import Downloader
from forge_optimizer import ForgeOptimizer
from name_resolver import NameResolver
from skyblock_api import SkyblockApi


def main():
  DataPlotter.show_bazaar('BOOSTER_COOKIE', complex=True)
  return


def init():
  SkyblockApi.init()
  Database.init()
  NameResolver.init()
  ForgeOptimizer.init()


if __name__ == '__main__':
  init()
  if len(sys.argv) == 1:
    main()
  elif sys.argv[1] == 'download_data':
    Downloader.download_and_save_data()
