from .paginatedResult import sanatizePaginatedParamValues, getPaginatedResultUsingIterator
import datetime

#Definition of Offset
# offset is the number to skip before outputing
# e.g. for 1,2,3,4,5,6,7,8,9 and pagesize 3
#  offset 0 is default and outputs 1,2,3
#  offset 1                outputs 2,3,4
#  offset 2                outputs 3,4,5

# Code to save JSON objects into a store.
#  Allows abstraction of particular store
#  This is the baseClass other stores inherit from
StoringNoneObjectAfterUpdateOperationException = Exception('Storing None Object After Update Operation')
SavedObjectShouldNotContainObjectVersionException = Exception('SavedObjectShouldNotContainObjectVersion')
SavingDateTimeTypeException = Exception('Saving datetime into objectstore not supported')

class WrongObjectVersionExceptionClass(Exception):
  pass
WrongObjectVersionException = WrongObjectVersionExceptionClass('Wrong object version supplied - Has another change occured since loading?')

class SuppliedObjectVersionWhenCreatingExceptionClass(Exception):
  pass
SuppliedObjectVersionWhenCreatingException = SuppliedObjectVersionWhenCreatingExceptionClass('Object verion was supplied but no current object found to be modified')

class ObjectStoreConfigError(Exception):
  pass

class MissingTransactionContextExceptionClass(Exception):
  pass
MissingTransactionContextException = MissingTransactionContextExceptionClass('Missing Transaction Context')

class UnallowedMutationExceptionClass(Exception):
  pass
UnallowedMutationException = UnallowedMutationExceptionClass("Trying to mutate objectStore without first starting a transaction")

class TriedToDeleteMissingObjectExceptionClass(Exception):
  pass
TriedToDeleteMissingObjectException = TriedToDeleteMissingObjectExceptionClass("Tried to delete non existant object")

class TryingToCreateExistingObjectExceptionClass(Exception):
  pass
TryingToCreateExistingObjectException = TryingToCreateExistingObjectExceptionClass("Tried to create an object that already exists")

class InvalidObjectTypeExceptionClass(Exception):
  pass
InvalidObjectTypeException = InvalidObjectTypeExceptionClass("Object Type Name is invalid")

def getDefaultGetSortKeyValueFn(outputFN):
  def defaultGetSortKeyValueFn(item, sortkeyName):
    return outputFN(item)[sortkeyName]
  return defaultGetSortKeyValueFn

'''
Calling pattern for connection where obj is and instance of ObjectStore:

storeConnection = obj.getConnectionContext()
def someFn(connectionContext):
  ##DO STUFF
storeConnection.executeInsideTransaction(someFn)

##Simplier
def someFn(connectionContext):
  ##DO STUFF
obj.executeInsideConnectionContext(someFn)

##Alternative (depreciated)
storeConnection = obj.getConnectionContext()
storeConnection.startTransaction()
try:
  ##DO STUFF
  storeConnection.commitTransaction()
except:
  storeConnection.rollbackTransaction()
  raise

'''

#Utility output functions
def outputFnJustKeys(item):
  (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = item
  #print("objectDICT:", objectDICT)
  #print("ObjectVersion:", ObjectVersion)
  #print("creationDate:", creationDate)
  #print("lastUpdateDate:", lastUpdateDate)
  #print("objectKey:", objectKey)
  return objectKey

#Output a tuple of the 5 items
def outputFnItems(item):
  return item

class ObjectStoreConnectionContext():
  #if object version is set to none object version checking is turned off
  # object version may be a number or a guid depending on store technology
  callsToStartTransaction = None
  def __init__(self):
    self.callsToStartTransaction = 0

  def _INT_startTransaction(self):
    if self.callsToStartTransaction != 0:
      raise Exception("Disabled ability for nexted transactions")
    self.callsToStartTransaction = self.callsToStartTransaction + 1
    return self._startTransaction()
  def _INT_commitTransaction(self):
    if self.callsToStartTransaction == 0:
      raise Exception("Trying to commit transaction but none started")
    self.callsToStartTransaction = self.callsToStartTransaction - 1
    return self._commitTransaction()
  def _INT_rollbackTransaction(self):
    if self.callsToStartTransaction == 0:
      raise Exception("Trying to rollback transaction but none started")
    self.callsToStartTransaction = self.callsToStartTransaction - 1
    return self._rollbackTransaction()

  def _INT_varifyWeCanMutateData(self):
    if self.callsToStartTransaction == 0:
      raise UnallowedMutationException


  def executeInsideTransaction(self, fnToExecute):
    retVal = None
    self._INT_startTransaction()
    try:
      retVal = fnToExecute(self)
      self._INT_commitTransaction()
    except:
      self._INT_rollbackTransaction()
      raise
    return retVal

  def validateObjectType(self, objectType):
    if '_' in objectType:
      raise InvalidObjectTypeException

  def validateObjectDict(self, objectToSTore):
    if objectToSTore is None:
      return
    if isinstance(objectToSTore, datetime.datetime):
      raise SavingDateTimeTypeException
    if isinstance(objectToSTore, dict):
      for x in objectToSTore:
        self.validateObjectDict(objectToSTore=objectToSTore[x])
    if isinstance(objectToSTore, list):
      for x in objectToSTore:
        self.validateObjectDict(objectToSTore=x)
    return

  #Return value is objectVersion of object saved
  def saveJSONObject(self, objectType, objectKey, JSONString, objectVersion = None):
    ret = self.saveJSONObjectV2(objectType, objectKey, JSONString, objectVersion)
    return ret[0] #just return object version

  #NEw version with Return value (newObjVersion, creationDateTime, lastUpdateDateTime)
  def saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion = None):
    self.validateObjectType(objectType=objectType)
    self.validateObjectDict(objectToSTore=JSONString)
    if 'ObjectVersion' in JSONString:
      raise SavedObjectShouldNotContainObjectVersionException
    self._INT_varifyWeCanMutateData()
    return self._saveJSONObjectV2(objectType, objectKey, JSONString, objectVersion)

  #Return value is None
  def removeJSONObject(self, objectType, objectKey, objectVersion = None, ignoreMissingObject = False):
    self._INT_varifyWeCanMutateData()
    return self._removeJSONObject(objectType, objectKey, objectVersion, ignoreMissingObject)

  # Update the object in single operation. make transaction safe??
  # Return value is same as saveJSONobject
  ## updateFn gets two paramaters:
  ##  object
  ##  connection
  #Seperate implementations not required as this uses save function
  def updateJSONObject(self, objectType, objectKey, updateFn, objectVersion = None):
    self._INT_varifyWeCanMutateData()

    obj, ver, creationDateTime, lastUpdateDateTime, objKey = self.getObjectJSON(objectType, objectKey)
    if objectVersion is None:
      #If object version is not supplied then assume update will not cause an error
      objectVersion = ver
    if str(objectVersion) != str(ver):
      raise WrongObjectVersionException
    obj = updateFn(obj, self)
    if obj is None:
      raise StoringNoneObjectAfterUpdateOperationException
    return self.saveJSONObject(objectType, objectKey, obj, objectVersion)

  #Return value is objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey
  #Return None, None, None, None, None if object isn't in store
  def getObjectJSON(self, objectType, objectKey):
    return self._getObjectJSON(objectType, objectKey)

  '''
  Example call - getting all data from backend:
    storeConnection = self.appObj.objectStore._getConnectionContext()
    def someFn(connectionContext):
      paginatedParamValues = {
        'offset': 0,
        'pagesize': 100000,
        'query': None,
        'sort': None,
      }
      return connectionContext.getPaginatedResult(objectType, paginatedParamValues=paginatedParamValues, outputFN=None)
    loadedData = storeConnection.executeInsideTransaction(someFn)
  '''

  def getPaginatedResult(self, objectType, paginatedParamValues, outputFN, filterFn=None, getSortKeyValueFn=None):
    if outputFN is None:
      outputFN = outputFnItems
    sanatizedPaginatedParamValues = sanatizePaginatedParamValues(paginatedParamValues)
    if filterFn is None:
      return self._getPaginatedResult(objectType, sanatizedPaginatedParamValues, outputFN)
    if getSortKeyValueFn is None:
      getSortKeyValueFn = getDefaultGetSortKeyValueFn(outputFN)
    return getPaginatedResultUsingIterator (
      iteratorObj=self._getPaginatedResultIterator(
        query=paginatedParamValues['query'],
        sort=paginatedParamValues['sort'],
        filterFN=filterFn,
        getSortKeyValueFn=getSortKeyValueFn,
        objectType=objectType
      ),
      outputFN=outputFN,
      offset=paginatedParamValues['offset'],
      pagesize=paginatedParamValues['pagesize']
    )

  # filterFN is applied first, then outputFN
  #  output fn is fed same params as getObjectJSON returns
  def getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    if outputFN is None:
      outputFN = outputFnItems
    def defFilter(item, whereClauseText):
      return True
    if filterFN is None:
      filterFN = defFilter
    return self._getAllRowsForObjectType(objectType, filterFN, outputFN, whereClauseText)


  def _saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion):
    raise Exception('_saveJSONObjectV2 Not Overridden')
  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    raise Exception('_removeJSONObject Not Overridden')
  def _getObjectJSON(self, objectType, objectKey):
    raise Exception('_getObjectJSON Not Overridden')
  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    raise Exception('_getPaginatedResult Not Overridden')
  def _getPaginatedResultIterator(self, query, sort, filterFN, getSortKeyValueFn, objectType):
    raise Exception('_getPaginatedResultIterator Not Overridden')

  #should return a fresh transaction context
  def _startTransaction(self):
    raise Exception('_startTransaction Not Overridden')
  def _commitTransaction(self):
    raise Exception('_commitTransaction Not Overridden')
  def _rollbackTransaction(self):
    raise Exception(' _rollbackTransaction Not Overridden')

  def _getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    raise Exception('_getAllRowsForObjectType Not Implemented for this objectstore type')

  #Optional override for closing the context
  def _close(self):
    pass

  def filterFN_basicTextInclusion(self, item, whereClauseText):
    #baseapp passes in whereClauseText as all upper case
    if whereClauseText is None:
      return True
    if whereClauseText == '':
      return True
    ###userDICT = CreateUserObjFromUserDict(appObj, item[0],item[1],item[2],item[3]).getJSONRepresenation()
    #TODO replace with a dict awear generic function
    #  we also need to consider removing spaces from consideration
    return whereClauseText in str(item).upper()

  def filterFN_allowAll(self, item, whereClauseText):
    return True

  #This won't work via a cache since not all objectTypes are read
  def list_all_objectTypes(self):
    return self._list_all_objectTypes()

  def _list_all_objectTypes(self):
    raise Exception('_list_all_objectTypes Not Overridden')

#Base class for object store
class ObjectStore():
  externalFns = None
  detailLogging = False
  type = None

  def __init__(self, externalFns, detailLogging, type):
    self.detailLogging = detailLogging
    self.type = type
    if 'getPaginatedResult' in externalFns:
      raise Exception("getPaginatedResult is supplied in external functions - new version of objectstore dosen't require it")
    self.externalFns = externalFns

  def detailLog(self, logMsg):
    if not self.detailLogging:
      return
    print("DETLOG:" + self.type + ":" + logMsg)

  def getConnectionContext(self):
    return self._getConnectionContext()
  def executeInsideConnectionContext(self, fnToExecute):
    context = self._getConnectionContext()
    a = None
    try:
      a = fnToExecute(context)
    finally:
      context._close()
    return a

  #helper function if we need just a single transaction in our contexts
  def executeInsideTransaction(self, fnToExecute):
    def dbfn(context):
      return context.executeInsideTransaction(fnToExecute)
    return self.executeInsideConnectionContext(dbfn)

  def _getConnectionContext(self):
    raise Exception('Not Overridden')

  def resetDataForTest(self):
    return self._resetDataForTest()

  #test only functions
  def _resetDataForTest(self):
    raise Exception('Not Overridden')
