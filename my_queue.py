class Queue:
  def __init__(self):
    self._list = []

  def __len__(self):
    return len(self._list)

  def enqueue(self, item):
    self._list.insert(0, item)

  def dequeue(self):
    return self._list.pop()

  def peek(self):
    return self._list[-1]
