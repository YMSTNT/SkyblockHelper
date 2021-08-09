
from skyblockl_api import SkyblockApi


def main():
  SkyblockApi.init()

  for prices in SkyblockApi.get_new_bazaar_prices():
    print(prices['lastUpdated'])


if __name__ == "__main__":
  main()
