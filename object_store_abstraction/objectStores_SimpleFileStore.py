from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, artificalRequestWithPaginationArgs, ObjectStoreConfigError
import os
import threading

# SimpleFileStore module
#  transactions not implemented

class ConnectionContextSimpleFileStorePrivateFns(ObjectStoreConnectionContext):
  def ensureObjectDirectoryExists(self, objectType, create):
    pass


class ConnectionContext(ConnectionContextSimpleFileStorePrivateFns):
  objectType = None
  def __init__(self, objectType):
    super(ConnectionContext, self).__init__()
    self.objectType = objectType

  #transactional memory not implemented
  def _startTransaction(self):
    pass
  def _commitTransaction(self):
    pass
  def _rollbackTransaction(self):
    pass


  def _saveJSONObject(self, objectType, objectKey, JSONString, objectVersion):
    '''
    dictForObjectType = self.objectType._INT_getDictForObjectType(objectType)
    curTimeValue = self.objectType.externalFns['getCurDateTime']()
    newObjectVersion = None
    if objectKey not in dictForObjectType:
      if objectVersion is not None:
        raise SuppliedObjectVersionWhenCreatingException
      newObjectVersion = 1
      dictForObjectType[objectKey] = (JSONString, newObjectVersion, curTimeValue, curTimeValue)
    else:
      #We have found an object in the DB
      if objectVersion is None:
        raise TryingToCreateExistingObjectException
      if str(objectVersion) != str(dictForObjectType[objectKey][1]):
        raise WrongObjectVersionException
      newObjectVersion = int(objectVersion) + 1
    dictForObjectType[objectKey] = (JSONString, newObjectVersion, dictForObjectType[objectKey][2], curTimeValue)
    return newObjectVersion
    '''
    pass

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    '''
    dictForObjectType = self.objectType._INT_getDictForObjectType(objectType)
    if objectVersion is not None:
      if objectKey not in dictForObjectType:
        if ignoreMissingObject:
          return None
        raise TriedToDeleteMissingObjectException
      if str(dictForObjectType[objectKey][1]) != str(objectVersion):
        raise WrongObjectVersionException
    if objectKey not in self.objectType._INT_getDictForObjectType(objectType):
      if ignoreMissingObject:
        return None
    del self.objectType._INT_getDictForObjectType(objectType)[objectKey]
    return None
    '''
    pass

  def _getObjectJSON(self, objectType, objectKey):
    '''
    objectTypeDict = self.objectType._INT_getDictForObjectType(objectType)
    if objectKey in objectTypeDict:
      return objectTypeDict[objectKey]
    return None, None, None, None
    '''
    pass

  def _filterFN(self, item, whereClauseText):
    '''
    if whereClauseText is None:
      return True
    if whereClauseText == '':
      return True
    ###userDICT = CreateUserObjFromUserDict(appObj, item[0],item[1],item[2],item[3]).getJSONRepresenation()
    #TODO replace with a dict awear generic function
    #  we also need to consider removing spaces from consideration
    return whereClauseText in str(item).upper()
    '''
    pass


  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    '''
    ##print('objectStoresMemory._getPaginatedResult self.objectType.objectData[objectType]:', self.objectType.objectData[objectType])
    srcData = []
    if objectType in self.objectType.objectData:
      srcData = self.objectType.objectData[objectType]
    return self.objectType.externalFns['getPaginatedResult'](
      srcData,
      outputFN,
      artificalRequestWithPaginationArgs(paginatedParamValues),
      self._filterFN
    )
    '''
    pass

# Class that will store objects as directoryies and files in the file system
class ObjectStore_SimpleFileStore(ObjectStore):
  baseLocation = ""
  fileAccessLock = None #Make sure we do one operation at a time

  def __init__(self, ConfigDict, externalFns):
    super(ObjectStore_SimpleFileStore, self).__init__(externalFns)

    self.fileAccessLock = threading.Lock()

    if "BaseLocation" not in ConfigDict:
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation param Missing")

    self.baseLocation = ConfigDict["BaseLocation"]

    if not os.path.exists(self.baseLocation):
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation not found")
    if not os.path.isdir(self.baseLocation):
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation is not a directory")

    #Write Test
    try:
      f = open(self.baseLocation + "/writetest.tmp","w+")
      f.write("Test")
      f.close()
      os.remove(self.baseLocation + "/writetest.tmp")
    except:
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - Write test to base location failed")

  def _INT_getDictForObjectType(self, objectType):
    if objectType not in self.objectData:
      #print("Creating dict for " + objectType)
      self.objectData[objectType] = dict()
    return self.objectData[objectType]

  def _getConnectionContext(self):
    return ConnectionContext(self)
