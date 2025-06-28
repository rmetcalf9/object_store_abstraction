from object_store_abstraction import ObjectStoreConnectionContext
from object_store_abstraction.DoubleStringIndex import ConnectionContext_Caching


class ConnectionContext(ObjectStoreConnectionContext):
  objectStore = None
  mainContext = None
  cachingContext = None
  def __init__(self, objectStore):
    super(ConnectionContext, self).__init__(doubleStringIndex=ConnectionContext_Caching(main_context=self))
    self.objectStore = objectStore
    self.cachingContext =  self.objectStore.cachingStore._getConnectionContext()
    self.mainContext =  self.objectStore.mainStore._getConnectionContext()

  def _startTransaction(self):
    retVal = self.mainContext._INT_startTransaction()
    return retVal
  def _commitTransaction(self):
    return self.mainContext._INT_commitTransaction()
  def _rollbackTransaction(self):
    return self.mainContext._INT_rollbackTransaction()

  def _saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion):
    (newObjVersion, creationDateTime, lastUpdateDateTime) = self.mainContext._saveJSONObjectV2(objectType, objectKey, JSONString, objectVersion)
    self.objectStore.getPolicy(objectType)._saveJSONObjectV2(
      objectType, objectKey, JSONString,
      objectVersion=newObjVersion,
      cacheContext=self.cachingContext,
      cullQueues=self.objectStore.cullQueues,
      creationDateTime=creationDateTime,
      lastUpdateDateTime=lastUpdateDateTime
    )
    return (newObjVersion, creationDateTime, lastUpdateDateTime)

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    ret = self.mainContext._removeJSONObject(objectType, objectKey, objectVersion, ignoreMissingObject)
    self.objectStore.getPolicy(objectType)._removeJSONObject(objectType, objectKey, objectVersion, ignoreMissingObject, cacheContext=self.cachingContext)
    return ret

  def _getObjectJSON(self, objectType, objectKey):
    return self.objectStore.getPolicy(objectType)._getObjectJSON(objectType, objectKey, cacheContext=self.cachingContext, mainContext=self.mainContext, cullQueues=self.objectStore.cullQueues)

  def _list_all_objectTypes(self):
    return self.mainContext._list_all_objectTypes()

  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    return self.mainContext._getPaginatedResult(objectType, paginatedParamValues, outputFN)

  def _getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    return self.mainContext._getAllRowsForObjectType(objectType, filterFN, outputFN, whereClauseText)

  def _getPaginatedResultIterator(self, query, sort, filterFN, getSortKeyValueFn, objectType):
    return self.mainContext._getPaginatedResultIterator(query, sort, filterFN, getSortKeyValueFn, objectType)

  def _truncateObjectType(self, objectType):
    # The _ skips the connectioncontext can mutate check
    self.mainContext.truncateObjectType(objectType)
    self.cachingContext._truncateObjectType(objectType)
