import queue
#for elem in list(q.queue)

class UniqueQueue():
  queue = None
  lookup = None
  def __init__(self, *args, **kwargs):
    self.queue = queue.Queue(*args, **kwargs)
    self.lookup = {}

  def put(self, item):
    if item in self.lookup:
      return None
    self.lookup[item] = "A"
    return self.queue.put(item)

  def get(self, *args, **kwargs):
    item = self.queue.get(*args, **kwargs)
    if item is None:
      return None
    if item in self.lookup:
      del self.lookup[item]
    return item

  def full(self):
    return self.queue.full()

  def qsize(self):
    return self.queue.qsize()

class CullQueuesClass():
  queues = None
  overhead = None
  def __init__(self):
    self.queues = {}
    self.overhead = 10

  def getQueue(self, objectType, maxsize):
    if objectType not in self.queues:
      self.queues[objectType] = UniqueQueue(maxsize=(maxsize + self.overhead))
    return self.queues[objectType]

