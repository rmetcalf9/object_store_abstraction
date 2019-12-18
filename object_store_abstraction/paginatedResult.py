#Function to return paginated result for query
from sortedcontainers import SortedDict
from .paginatedResultIterator import PaginatedResultIteratorFromDictWithAttrubtesAsKeysClass

def isValue(val):
  if val is None:
    return False
  if len(val.strip())==0:
    return False
  return True

def sanatizePaginatedParamValues(origValues):
  pagesizemax = 100

  offset = -1  #number from 0 upwards
  pagesize = -1 #number from 0 upwards
  query = None #none or string not empty
  sort = None #none or string not empty

  #'offset': offset,
  if origValues['offset'] is None:
    offset = 0
  else:
    offset = int(origValues['offset'])

  #'pagesize': pagesize,
  if origValues['pagesize'] is None:
    pagesize = 100
  else:
    pagesize = int(origValues['pagesize'])

  if pagesize > pagesizemax:
    pagesize = pagesizemax

  #'query': query,
  if isValue(origValues['query']):
    query = origValues['query']

  #'sort': sort,
  if isValue(origValues['sort']):
    sort = origValues['sort']


  return {
    'offset': offset,
    'pagesize': pagesize,
    'query': query,
    'sort': sort,
  }

#iterator gets, query, filterFN and sort
def getPaginatedResult(
  list,
  outputFN,
  offset,
  pagesize,
  query,
  sort,
  filterFN
):
  def getSortKeyValueFn(item, sortkeyName):
    return outputFN(item)[sortkeyName]
  return getPaginatedResultUsingIterator (
    iteratorObj=PaginatedResultIteratorFromDictWithAttrubtesAsKeysClass(list, query, sort, filterFN, getSortKeyValueFn),
    outputFN=outputFN,
    offset=offset,
    pagesize=pagesize
  )



def getPaginatedResultUsingIterator(
  iteratorObj,
  outputFN,
  offset,
  pagesize
):
  def internalFilterFN(obj):
    return filterFn(obj)

  output = []
  totalRowsOutput = 0
  curPos = 0
  continueLooking = True
  while continueLooking:
    obj = iteratorObj.next()
    if obj is None:
      #print("None Stop")
      continueLooking = False
    else:
      if curPos >= offset:
        output.append(outputFN(obj))
        totalRowsOutput = totalRowsOutput + 1
      curPos = curPos + 1
      if totalRowsOutput >= pagesize:
        #print("PS Stop")
        continueLooking = False
        curPos = curPos + 1 # show that there is more to query

  return {
    'pagination': {
      'offset': offset,
      'pagesize': pagesize,
      'total': curPos
    },
    'result': output
  }

def getPaginatedResultUsingPythonIterator(
  iteratorObj,
  outputFN,
  offset,
  pagesize
):
  output = []
  totalRowsOutput = 0
  curPos = 0
  for x in iteratorObj:
    if curPos >= offset:
      output.append(outputFN(x))
      totalRowsOutput = totalRowsOutput + 1
      if totalRowsOutput >= pagesize:
        curPos = curPos + 2 # show that there is more to query
        return {
          'pagination': {
            'offset': offset,
            'pagesize': pagesize,
            'total': curPos
          },
          'result': output
        }
    curPos = curPos + 1
  return {
    'pagination': {
      'offset': offset,
      'pagesize': pagesize,
      'total': curPos
    },
    'result': output
  }
