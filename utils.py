from datetime import datetime
from time import time


class Utils:

  quitting = False

  @staticmethod
  def epoch_to_human_time(epoch: int):
    return datetime.fromtimestamp(epoch / 1000).strftime('%m-%d %H:%M:%S')

  @staticmethod
  def log(text: str, epoch=None):
    if not epoch:
      epoch = time()
    print(f'[{Utils.epoch_to_human_time(epoch)}] {text}')
