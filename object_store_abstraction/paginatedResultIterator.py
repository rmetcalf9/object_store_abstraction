

class PaginatedResultIteratorBaseClass():
  whereClauses = None
  filterFn = None

  def __init__(self, query, filterFn):
    if query is not None:
      if query != "":
        self.whereClauses = query.strip().upper().split(" ")
    self.filterFn = filterFn

  def includeResult(self, res):
    # print("paginatedResItBase includeResult Start", res)
    if res is None:
      return True # we get a none at the end - it's the stop signal
    if self.whereClauses is None:
      if self.filterFn is None:
        return True
      return self.filterFn(res, None) # Filter function with no where clauses
    for curClause in self.whereClauses:
      if not self.filterFn(res, curClause):
        return False
    return True

  def next(self):
    a = self._next()
    while not self.includeResult(a):
      a = self._next()
      if a is None:
        return None
    return a

  def _next(self):
    raise Exception("_next is Not Implemented")

def sortListOfKeysToDictBySortString(listOfKeys, dictOfData, sortString, getSortKeyValueFn):
  def getSortTuple(key):
    #sort keys are case sensitive
    kk = key.split(":")
    if len(kk)==0:
      raise Exception('Invalid sort key')
    elif len(kk)==1:
      return {'name': kk[0], 'desc': False}
    elif len(kk)==2:
      if kk[1].lower() == 'desc':
        return {'name': kk[0], 'desc': True}
      elif kk[1].lower() == 'asc':
        return {'name': kk[0], 'desc': False}
    raise Exception('Invalid sort key - ' + key)

  def genSortKeyGenFn(listBeingSorted, sortkey):
    def sortKeyGenFn(ite):
      try:
        # print(sortkey)
        # print(outputFN(listBeingSorted[ite])[sortkey])
        ret = getSortKeyValueFn(listBeingSorted[ite], sortkey)
        if ret is None:
          return ''
        if isinstance(ret, int):
          return ('000000000000000000000000000000000000000000000000000' + str(ret))[-50:]
        if isinstance(ret, bool):
          if ret:
            return 'True'
          return 'False'
        return ret
      except KeyError:
        raise Exception('Sort key ' + sortkey + ' not found')
    return sortKeyGenFn

  # sort by every sort key one at a time starting with the least significant
  for curSortKey in sortString.split(",")[::-1]:
    sk = getSortTuple(curSortKey)
    listOfKeys.sort(key=genSortKeyGenFn(dictOfData, sk['name']), reverse=sk['desc'])

class PaginatedResultIteratorFromDictWithAttrubtesAsKeysClass(PaginatedResultIteratorBaseClass):
  dict = None
  curIdx = None
  listOfKeys = None

  def __init__(self, dict, query, sort, filterFN, getSortKeyValueFn):
    PaginatedResultIteratorBaseClass.__init__(self, query, filterFN)
    self.curIdx = 0
    self.dict = dict
    self.listOfKeys = []
    for cur in self.dict:
      self.listOfKeys.append(cur)

    if sort is not None:
      sortListOfKeysToDictBySortString(self.listOfKeys, self.dict, sort, getSortKeyValueFn)

  #def hasMore(self):
  #  return self.curIdx < len(self.listOfKeys)

  def _next(self):
    self.curIdx = self.curIdx + 1
    if self.curIdx > len(self.listOfKeys):
      return None
    return self.dict[self.listOfKeys[self.curIdx-1]]
