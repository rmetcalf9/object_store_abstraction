from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException
from .paginatedResult import getPaginatedResult
from .paginatedResultIterator import PaginatedResultIteratorBaseClass, sortListOfKeysToDictBySortString

class ConnectionContext(ObjectStoreConnectionContext):
  objectStore = None
  def __init__(self, objectStore):
    super(ConnectionContext, self).__init__()
    self.objectStore = objectStore

  #transactional memory not implemented
  def _startTransaction(self):
    pass
  def _commitTransaction(self):
    pass
  def _rollbackTransaction(self):
    pass


  def _saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion):
    dictForObjectType = self.objectStore._INT_getDictForObjectType(objectType)
    curTimeValue = self.objectStore.externalFns['getCurDateTime']()
    newObjectVersion = None
    if objectKey not in dictForObjectType:
      if objectVersion is not None:
        raise SuppliedObjectVersionWhenCreatingException
      newObjectVersion = 1
      dictForObjectType[objectKey] = (JSONString, newObjectVersion, curTimeValue, curTimeValue, objectKey)
    else:
      #We have found an object in the DB
      if objectVersion is None:
        raise TryingToCreateExistingObjectException
      if str(objectVersion) != str(dictForObjectType[objectKey][1]):
        raise WrongObjectVersionException
      newObjectVersion = int(objectVersion) + 1
    dictForObjectType[objectKey] = (JSONString, newObjectVersion, dictForObjectType[objectKey][2], curTimeValue, objectKey)
    return (newObjectVersion, dictForObjectType[objectKey][2], curTimeValue)

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    dictForObjectType = self.objectStore._INT_getDictForObjectType(objectType)
    if objectVersion is not None:
      if objectKey not in dictForObjectType:
        if ignoreMissingObject:
          return None
        raise TriedToDeleteMissingObjectException
      if str(dictForObjectType[objectKey][1]) != str(objectVersion):
        raise WrongObjectVersionException
    if objectKey not in self.objectStore._INT_getDictForObjectType(objectType):
      if ignoreMissingObject:
        return None
    del self.objectStore._INT_getDictForObjectType(objectType)[objectKey]
    return None

  def _getObjectJSON(self, objectType, objectKey):
    objectTypeDict = self.objectStore._INT_getDictForObjectType(objectType)
    if objectKey in objectTypeDict:
      return objectTypeDict[objectKey]
    return None, None, None, None, None

  def _list_all_objectTypes(self):
    results = []
    for x in self.objectStore.objectData:
      results.append(x)
    return results

  def __getAllRowsForObjectType(self, objectType):
    srcData = []
    if objectType in self.objectStore.objectData:
      srcData = self.objectStore.objectData[objectType]
    return srcData

  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    ##print('objectStoresMemory._getPaginatedResult self.objectType.objectData[objectType]:', self.objectType.objectData[objectType])
    srcData = self.__getAllRowsForObjectType(objectType)

    return getPaginatedResult(
      list=srcData,
      outputFN=outputFN,
      offset=paginatedParamValues['offset'],
      pagesize=paginatedParamValues['pagesize'],
      query=paginatedParamValues['query'],
      sort=paginatedParamValues['sort'],
      filterFN=self.filterFN_basicTextInclusion
    )

  def _getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    superObj = self.__getAllRowsForObjectType(objectType)
    outputLis = []
    for curKey in superObj:
      if self.filterFN_basicTextInclusion(superObj[curKey], whereClauseText):
        if filterFN(superObj[curKey], whereClauseText):
          outputLis.append(superObj[curKey])
    return list(map(outputFN, outputLis))

  def _getPaginatedResultIterator(self, query, sort, filterFN, getSortKeyValueFn, objectType):
    return Iterator(query, sort, filterFN, getSortKeyValueFn, self, objectType)

# Class that will store objects in memory only
class ObjectStore_Memory(ObjectStore):
  objectData = None
  def __init__(self, configJSON, externalFns, detailLogging, type, factoryFn):
    super(ObjectStore_Memory, self).__init__(externalFns, detailLogging, type)
    self.objectData = dict()
    #Dict = (objDICT, objectVersion, creationDate, lastUpdateDate)

  def _INT_getDictForObjectType(self, objectType):
    if objectType not in self.objectData:
      #print("Creating dict for " + objectType)
      self.objectData[objectType] = dict()
    return self.objectData[objectType]

  def _getConnectionContext(self):
    return ConnectionContext(self)

class Iterator(PaginatedResultIteratorBaseClass):
  objectKeys = None
  curIdx = None
  memoryStoreConnectionContext = None
  objectType = None

  def __init__(self, query, sort, filterFN, getSortKeyValueFn, memoryStoreConnectionContext, objectType):
    PaginatedResultIteratorBaseClass.__init__(self, query, filterFN)
    self.memoryStoreConnectionContext = memoryStoreConnectionContext
    self.objectType = objectType
    self.curIdx = 0

    self.objectKeys = list(self.memoryStoreConnectionContext.objectStore._INT_getDictForObjectType(objectType).keys())
    if sort is not None:
      sortListOfKeysToDictBySortString(self.objectKeys, self.memoryStoreConnectionContext.objectStore._INT_getDictForObjectType(objectType), sort, getSortKeyValueFn)

  def _next(self):
    self.curIdx = self.curIdx + 1
    if self.curIdx > len(self.objectKeys):
      return None
    return self.memoryStoreConnectionContext._getObjectJSON(self.objectType, self.objectKeys[self.curIdx-1])
