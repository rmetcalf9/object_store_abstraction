from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, ObjectStoreConfigError
from .paginatedResult import getPaginatedResult

'''
Migrating store type handles moving datastore from one type to another
It keeps two stores, a from store and a to store.
All reads are made from the from stores
Writes are mirrored to both the from and to stores
This allows for live migrations and once migration is complete
users can switch to a version using just the to store

Note: The to store should start life completly empty otherwise keys that are
already there may never be migrated
'''

class ConnectionContext(ObjectStoreConnectionContext):
  objectStore = None
  fromContext = None
  toContext = None
  def __init__(self, objectStore):
    super(ConnectionContext, self).__init__()
    self.objectStore = objectStore
    self.fromContext = self.objectStore.fromStore._getConnectionContext()
    self.toContext = self.objectStore.toStore._getConnectionContext()

  #transactional memory not implemented
  def _startTransaction(self):
    self.toContext._startTransaction()
    return self.fromContext._startTransaction()
  def _commitTransaction(self):
    self.toContext._commitTransaction()
    return self.fromContext._commitTransaction()
  def _rollbackTransaction(self):
    self.toContext._rollbackTransaction()
    return self.fromContext._rollbackTransaction()

  def _saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion):
    to_objectDICT, to_ObjectVersion, to_creationDate, to_lastUpdateDate, to_objectKey = self.toContext._getObjectJSON(objectType, objectKey)
    if to_objectDICT is None:
      self.toContext._saveJSONObjectV2(objectType, objectKey, JSONString, objectVersion=None)
    else:
      self.toContext._saveJSONObjectV2(objectType, objectKey, JSONString, objectVersion=to_ObjectVersion)
    return self.fromContext._saveJSONObjectV2(objectType, objectKey, JSONString, objectVersion)

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    to_objectDICT = None
    to_objectDICT, to_ObjectVersion, to_creationDate, to_lastUpdateDate, to_objectKey = self.toContext._getObjectJSON(objectType, objectKey)
    if to_objectDICT is not None:
      #no need to remove from to stroe if it is not in tostore
      self.toContext._removeJSONObject(objectType, objectKey, to_ObjectVersion, ignoreMissingObject)
    return self.fromContext._removeJSONObject(objectType, objectKey, objectVersion, ignoreMissingObject)

  def _getObjectJSON(self, objectType, objectKey):
    return self.fromContext._getObjectJSON(objectType, objectKey)

  def _list_all_objectTypes(self):
    return self.fromContext._list_all_objectTypes()

  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    return self.fromContext._getPaginatedResult(objectType, paginatedParamValues, outputFN)

  def _getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    return self.fromContext._getAllRowsForObjectType(objectType, filterFN, outputFN, whereClauseText)

  def _getPaginatedResultIterator(self, query, sort, filterFN, getSortKeyValueFn, objectType):
    return self.fromContext._getPaginatedResultIterator(query, sort, filterFN, getSortKeyValueFn, objectType)

# Class that will store objects in memory only
class ObjectStore_Migrating(ObjectStore):
  fromStore = None
  toStore = None
  def __init__(self, configJSON, externalFns, detailLogging, type, factoryFn):
    super(ObjectStore_Migrating, self).__init__(externalFns, detailLogging, type)

    requiredConfigItems = ['To','From']
    for x in requiredConfigItems:
      if x not in configJSON:
        raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Migrating ERROR - config param " + x + " missing")
      if x is None:
        raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Migrating ERROR - config param " + x + " can't be none")

    self.fromStore = factoryFn(
      configJSON["From"],
      externalFns,
      detailLogging=detailLogging
    )
    self.toStore = factoryFn(
      configJSON["To"],
      externalFns,
      detailLogging=detailLogging
    )


  def _getConnectionContext(self):
    return ConnectionContext(self)
