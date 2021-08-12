
import sys

from data_plotter import DataPlotter
from database import Database
from downloader import Downloader
from skyblock_api import SkyblockApi


def main():
  DataPlotter.show_bazaar('BOOSTER_COOKIE', complex=True)
  return


def init():
  SkyblockApi.init()
  Database.init()
  # Database.setup()


if __name__ == '__main__':
  init()
  if len(sys.argv) == 1:
    main()
  elif sys.argv[1] == 'download_data':
    Downloader.download_and_save_data()
