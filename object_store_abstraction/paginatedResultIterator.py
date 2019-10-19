

class PaginatedResultIteratorBaseClass():
  def hasMore(self):
    raise Exception("Not Implemented")

  def next(self):
    raise Exception("Not Implemented")

class PaginatedResultIteratorFromDictWithAttrubtesAsKeysClass(PaginatedResultIteratorBaseClass):
  dict = None
  curIdx = None
  listOfKeys = None

  def __init__(self, dict, query, sort, filterFN):
    self.curIdx = 0
    self.dict = dict
    self.listOfKeys = []
    for cur in self.dict:
      self.listOfKeys.append(cur)

  def hasMore(self):
    return self.curIdx < len(self.listOfKeys)

  def next(self):
    self.curIdx = self.curIdx + 1
    return self.dict[self.listOfKeys[self.curIdx-1]]
