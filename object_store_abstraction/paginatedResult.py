#Function to return paginated result for query
from sortedcontainers import SortedDict

def getPaginatedResult(
  list,
  outputFN,
  offset,
  pagesize,
  query,
  sort,
  filterFN
):
  pagesizemax = 100
  if offset is None:
    offset = 0
  else:
    offset = int(offset)
  if pagesize is None:
    pagesize = 100
  else:
    pagesize = int(pagesize)

  # limit rows returned per request
  if pagesize > pagesizemax:
    pagesize = pagesizemax

  if query is not None:
    origList = SortedDict(list)
    list = SortedDict()
    where_clauses = query.strip().upper().split(" ")
    def includeItem(item):
      for curClause in where_clauses:
        if not filterFN(item, curClause):
          return False
      return True
    for cur in origList:
      if includeItem(origList[cur]):
        list[cur]=origList[cur]

  # we now have "list" which is a filtered down list of things we need to return
  #construct a list of keys to the object, all null
  sortedKeys = []
  for cur in list:
    sortedKeys.append(cur)

  #Sort sortedKeys
  if sort is not None:
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
          ret = outputFN(listBeingSorted[ite])[sortkey]
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
    for curSortKey in sort.split(",")[::-1]:
      sk = getSortTuple(curSortKey)
      sortedKeys.sort(key=genSortKeyGenFn(list, sk['name']), reverse=sk['desc'])

  output = []
  for cur in range(offset, (pagesize + offset)):
    if (cur<len(list)):
      output.append(outputFN(list[sortedKeys[cur]]))
      #output.append(outputFN(list[list.keys()[sortedKeys[cur]]]))

  return {
    'pagination': {
      'offset': offset,
      'pagesize': pagesize,
      'total': len(list)
    },
    'result': output
  }
