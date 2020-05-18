import queue

class CullQueuesClass():
  queues = None
  overhead = None
  def __init__(self):
    self.queues = {}
    self.overhead = 10

  def getQueue(self, objectType, maxsize):
    if objectType not in self.queues:
      self.queues[objectType] = queue.Queue(maxsize=(maxsize + self.overhead))
    return self.queues[objectType]

