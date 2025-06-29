from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, ObjectStoreConfigError
import os
import threading
import base64
from dateutil.parser import parse
import pytz
from .paginatedResult import getPaginatedResult
from .paginatedResultIterator import PaginatedResultIteratorBaseClass, sortListOfKeysToDictBySortString
from .DoubleStringIndex import ConnectionContext_SimpleFileStore

import shutil #For remove dir and contents

import sys

# SimpleFileStore module
#  transactions not implemented

#I originoally used os.scandir but this is not supported in python 3.6
#  so I have abstracted it to this function
#  this returns a list of the names
def localScanDir(directoryToScan, OnlyReturnDirectories):
  subDirsToDel = []

  isAtLeasePython3_6 = False
  pythonMajorVersion = sys.version_info[0]
  pythonMinorVersion = sys.version_info[1]
  if pythonMajorVersion > 2:
    if pythonMinorVersion > 5:
      isAtLeasePython3_6 = True

  if isAtLeasePython3_6:
    with os.scandir(directoryToScan) as entries:
      for entry in entries:
        if entry.is_dir():
          subDirsToDel.append(entry.name)
        else:
          if not OnlyReturnDirectories:
            subDirsToDel.append(entry.name)
  else:
    fsEntries = os.listdir( directoryToScan )
    for x in fsEntries:
      if not OnlyReturnDirectories:
        subDirsToDel.append(x)
      else:
        if os.path.isdir(directoryToScan + "/" + x):
          subDirsToDel.append(x)

  return subDirsToDel

class ConnectionContextSimpleFileStorePrivateFns(ObjectStoreConnectionContext):
  objectStore = None
  def __init__(self, objectStore):
    super(ConnectionContextSimpleFileStorePrivateFns, self).__init__(doubleStringIndex=ConnectionContext_SimpleFileStore(main_context=self))
    self.objectStore = objectStore

  #Return the object type directory or None
  def getObjectTypeDirectory(self, objectType, createObjectDir):
    dirString =  self.objectStore.baseLocation + "/" + self.objectStore.directoryNamePrefix + self.objectStore.getFileSystemSafeStringFromKey(objectType)
    if self.objectStore.isKnownObjectType(objectType):
      return dirString
    if os.path.exists(dirString):
      self.objectStore.setKnownObjectType(objectType)
      return dirString
    if not createObjectDir:
      return None
    os.mkdir(dirString)
    self.objectStore.setKnownObjectType(objectType)
    return dirString

  def _list_all_objectTypes(self):
    with self.objectStore.fileAccessLock:
      lis = localScanDir(self.objectStore.baseLocation, OnlyReturnDirectories=True)
    results = []
    for x in lis:
      if x.startswith(self.objectStore.directoryNamePrefix):
        results.append(self.objectStore.getKeyFromFileSystemSafeString(x[1:]))
    return results

  def getObjectFile(self, objectType, objectKey, createObjectDir):
    dirString = self.getObjectTypeDirectory(objectType, createObjectDir)
    if dirString is None:
      return None
    return dirString + "/" + self.objectStore.getFileSystemSafeStringFromKey(objectKey)

  def getObjectJSONWithoutLock(self, objectType, objectKey):
    fileName = self.getObjectFile(objectType, objectKey, False)
    if fileName is None:
      return None, None, None, None, None

    if not os.path.exists(fileName):
      return None, None, None, None, None

    fileO = open(fileName, 'r')
    filecontent = fileO.read()
    fileO.close()
    ##print(filecontent)
    filecontentDict = eval(filecontent)

    #dictForObjectType[objectKey] = (JSONString, newObjectVersion, curTimeValue, curTimeValue)
    dt = parse(filecontentDict["Create"])
    creationDate = dt.astimezone(pytz.utc)
    dt = parse(filecontentDict["LastUpdate"])
    lastUpdateDate = dt.astimezone(pytz.utc)
    return filecontentDict["Data"], filecontentDict["ObjVer"], creationDate, lastUpdateDate, objectKey

  def _truncateObjectType(self, objectType):
    dirString = self.getObjectTypeDirectory(objectType, False)
    if dirString is None:
      return
    self.objectStore.removeKnownObjectType(objectType)
    shutil.rmtree(dirString)

class ConnectionContext(ConnectionContextSimpleFileStorePrivateFns):

  #transactional memory not implemented
  def _startTransaction(self):
    pass
  def _commitTransaction(self):
    pass
  def _rollbackTransaction(self):
    pass


  def _saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion):
    createDateToReturn = None
    lastUpdateDataToReturn = None
    with self.objectStore.fileAccessLock:
      curTime = self.objectStore.externalFns['getCurDateTime']()
      curTimeValue = curTime.isoformat()

      (o_objectDICT, o_ObjectVersion, o_creationDate, o_lastUpdateDate, o_objectKey) = self.getObjectJSONWithoutLock(objectType, objectKey)

      newObjectVersion = None
      createDate = None
      updateDate = None

      if o_ObjectVersion is None:
        # Object dosen't exist we are creating it
        if objectVersion is not None:
          raise SuppliedObjectVersionWhenCreatingException
        newObjectVersion = 1
        createDate = curTimeValue
        updateDate = curTimeValue
        createDateToReturn = curTime
        lastUpdateDataToReturn = curTime
      else:
        # OBject exists we are updating it
        if objectVersion is None:
          raise TryingToCreateExistingObjectException
        #print("1:", str(objectVersion), ":", o_ObjectVersion, ":")
        if (int(str(objectVersion)) != int(o_ObjectVersion)):
          raise WrongObjectVersionException
        newObjectVersion = int(objectVersion) + 1
        createDate = o_creationDate.isoformat()
        updateDate = curTimeValue
        createDateToReturn = o_creationDate
        lastUpdateDataToReturn = curTime

      DictToSave = {
        "Data": JSONString,
        "ObjVer": newObjectVersion,
        "Create": createDate,
        "LastUpdate": updateDate
      }

      fileName = self.getObjectFile(objectType, objectKey, True)

      target = open(fileName, 'w') #w mode overwrites file content
      target.write(str(DictToSave))
      target.close()

    return (newObjectVersion, createDateToReturn, lastUpdateDataToReturn)

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    if not self.objectStore.isKnownObjectType(objectType):
      if ignoreMissingObject:
        return None
      raise TriedToDeleteMissingObjectException
    if objectVersion is not None:
      (o_objectDICT, o_ObjectVersion, o_creationDate, o_lastUpdateDate, o_objectKey) = self.getObjectJSONWithoutLock(objectType, objectKey)
      if o_objectDICT is not None:
        if str(o_ObjectVersion) != str(objectVersion):
          raise WrongObjectVersionException

    with self.objectStore.fileAccessLock:
      #Simple way of doing it - call a get and see what is returned
      #(o_objectDICT, o_ObjectVersion, o_creationDate, o_lastUpdateDate) = self.getObjectJSONWithoutLock(objectType, objectKey)
      #if o_objectDICT is None:
      #  if ignoreMissingObject:
      #    return None
      #  raise TriedToDeleteMissingObjectException

      #It is more efficient to check the file exists - we don't need to get it
      fileName = self.getObjectFile(objectType, objectKey, True)
      if fileName is None:
        if ignoreMissingObject:
          return None
        raise TriedToDeleteMissingObjectException
      if not os.path.exists(fileName):
        if ignoreMissingObject:
          return None
        raise TriedToDeleteMissingObjectException

      os.remove(fileName)
    return None

  #Return value is objectDICT, ObjectVersion, creationDate, lastUpdateDate
  #Return None, None, None, None if object isn't in store
  def _getObjectJSON(self, objectType, objectKey):
    with self.objectStore.fileAccessLock:
      a = self.getObjectJSONWithoutLock(objectType, objectKey)
    return a


  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    collectedObjects = self.__getAllRowsForObjectType(objectType)

    ##print('objectStoresMemory._getPaginatedResult self.objectType.objectData[objectType]:', self.objectType.objectData[objectType])
    return getPaginatedResult(
      list=collectedObjects,
      outputFN=outputFN,
      offset=paginatedParamValues['offset'],
      pagesize=paginatedParamValues['pagesize'],
      query=paginatedParamValues['query'],
      sort=paginatedParamValues['sort'],
      filterFN=self.filterFN_basicTextInclusion
    )

  def __getAllRowsForObjectType(self, objectType):
    #This is as simple as getting a directory listing
    collectedObjects = {}

    otd = self.getObjectTypeDirectory(objectType, createObjectDir=False)
    if otd is not None:
      objectFiles = localScanDir(otd, False)
      for curFileName in objectFiles:
        #print(otd + "/" + curFileName + "->" + getKeyFromFileSystemSafeString(curFileName))
        objectKey = self.objectStore.getKeyFromFileSystemSafeString(curFileName)
        #print("OK:" + objectKey)
        (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = self._getObjectJSON(objectType, objectKey)
        collectedObjects[objectKey] = (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey)

    return collectedObjects

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


# Class that will store objects as directoryies and files in the file system
class ObjectStore_SimpleFileStore(ObjectStore):
  baseLocation = ""
  fileAccessLock = None #Make sure we do one operation at a time
  # using with when accessing see https://www.bogotobogo.com/python/Multithread/python_multithreading_Using_Locks_with_statement_Context_Manager.php
  #  this deals properly with exceptions

  directoryNamePrefix = "_"
  knownExistingObjectTypes = None #reduce os.dir exist calls by storeing objects we know have a dir

  # Index files are for the DoubleStringIndex storage
  directoryNamePrefixDoubleStringIndex = "i"
  knownExistingIndexTypes = None #reduce os.dir exist calls by storeing objects we know have a dir

  def getFileSystemSafeStringFromKey(self, key):
    b64enc = base64.b64encode(key.encode('utf-8')).decode()
    return b64enc.replace("/", "_").replace("+", ":")

  def getKeyFromFileSystemSafeString(self, fss):
    b64enc = fss.replace("_", "/").replace(":", "+")
    return base64.b64decode(b64enc.encode('utf-8')).decode('utf-8')

  def isKnownObjectType(self, objectType):
    return objectType in self.knownExistingObjectTypes
  def setKnownObjectType(self, objectType):
    self.knownExistingObjectTypes[objectType] = True
  def removeKnownObjectType(self, objectType):
    del self.knownExistingObjectTypes[objectType]

  def isKnownIndexType(self, indexType):
    return indexType in self.knownExistingIndexTypes
  def setKnownIndexType(self, indexType):
    self.knownExistingIndexTypes[indexType] = True
  def removeKnownIndexType(self, indexType):
    if indexType in self.knownExistingIndexTypes:
      del self.knownExistingIndexTypes[indexType]

  def __init__(self, ConfigDict, externalFns, detailLogging, type, factoryFn):
    super(ObjectStore_SimpleFileStore, self).__init__(externalFns, detailLogging, type)

    self.fileAccessLock = threading.Lock()
    self.knownExistingObjectTypes = {}
    self.knownExistingIndexTypes = {}

    if "BaseLocation" not in ConfigDict:
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation param Missing")

    self.baseLocation = ConfigDict["BaseLocation"]

    if self.baseLocation[-1] == "/":
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation should not end with slash")
    if self.baseLocation[-1] == "\\":
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation should not end with backslash")

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

    # Scan base directory and load in all known object types
    subDirs = localScanDir(directoryToScan=self.baseLocation, OnlyReturnDirectories=True)
    for curDir in subDirs:
      if curDir.startswith(self.directoryNamePrefix):
        self.setKnownObjectType(self.getKeyFromFileSystemSafeString(curDir[len(self.directoryNamePrefix):]))

  #required for unit testing
  # using transaction even though they are not supported
  def _resetDataForTest(self):
    def someFn(connectionContext):
      subDirsToDel = localScanDir(directoryToScan=self.baseLocation, OnlyReturnDirectories=True)

      for curDir in subDirsToDel:
        if curDir.startswith(self.directoryNamePrefix):
          shutil.rmtree(self.baseLocation + "/" + curDir)
        if curDir.startswith(self.directoryNamePrefixDoubleStringIndex):
          shutil.rmtree(self.baseLocation + "/" + curDir)

    with self.fileAccessLock:
      self.knownExistingObjectTypes = {}
      # print("SFS 279: reseting all known types")
      self.executeInsideTransaction(someFn)

  def _getConnectionContext(self):
    return ConnectionContext(self)

class Iterator(PaginatedResultIteratorBaseClass):
  objectKeys = None
  objects = None # load the objects - only required if we are sorting
  curIdx = None
  simpleFileStoreConnectionContext = None
  objectType = None
  def __init__(self, query, sort, filterFN, getSortKeyValueFn, simpleFileStoreConnectionContext, objectType):
    PaginatedResultIteratorBaseClass.__init__(self, query, filterFN)
    self.simpleFileStoreConnectionContext = simpleFileStoreConnectionContext
    self.objectType = objectType
    self.objectFiles = []
    otd = simpleFileStoreConnectionContext.getObjectTypeDirectory(objectType, createObjectDir=False)
    objFilenames = []
    if otd is not None:
      objFilenames = localScanDir(otd, False)
    self.objectKeys = []
    for curObjFilename in objFilenames:
      self.objectKeys.append(self.simpleFileStoreConnectionContext.objectStore.getKeyFromFileSystemSafeString(curObjFilename))

    if sort is not None:
      # Load all objects into dict indexed by key
      self.objects = {}
      for curKey in self.objectKeys:
        self.objects[curKey] = self.simpleFileStoreConnectionContext._getObjectJSON(self.objectType, curKey)

      sortListOfKeysToDictBySortString(self.objectKeys, self.objects, sort, getSortKeyValueFn)


    self.curIdx = 0

    #print(self.objectFiles)

  def _next(self):
    self.curIdx = self.curIdx + 1
    if self.curIdx > len(self.objectKeys):
      return None


    #print("OK:" + objectKey)
    if self.objects is None:
      return self.simpleFileStoreConnectionContext._getObjectJSON(self.objectType, self.objectKeys[self.curIdx-1])
    else:
      # All objects were loaded so just output the object
      return self.objects[self.objectKeys[self.curIdx-1]]
