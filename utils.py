from datetime import datetime
from time import sleep, time


class Utils:

  quitting = False
  paused = False

  @staticmethod
  def epoch_to_human_time(epoch: int):
    return datetime.fromtimestamp(epoch / 1000).strftime('%m-%d %H:%M:%S')

  @staticmethod
  def log(message: str, epoch=None, save=False):
    if not epoch:
      epoch = time() * 1000
    text = f'[{Utils.epoch_to_human_time(epoch)}] {message}'
    print(text)
    if save:
      with open('data/logs/log.txt', 'a') as f:
        f.write(f'{text}\n')

  @staticmethod
  def sleep_while(condition_func, seconds: float):
    for _ in range(int(seconds * 10)):
      if not condition_func():
        break
      sleep(0.1)
