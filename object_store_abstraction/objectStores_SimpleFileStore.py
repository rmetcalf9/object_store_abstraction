from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, artificalRequestWithPaginationArgs, ObjectStoreConfigError
import os
import threading
import base64
from dateutil.parser import parse
import pytz

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

def getFileSystemSafeStringFromKey(key):
  b64enc = base64.b64encode(key.encode('utf-8')).decode()
  return b64enc.replace("/","_").replace("+",":")

def getKeyFromFileSystemSafeString(fss):
  b64enc = fss.replace("_","/").replace(":","+")
  return base64.b64decode(b64enc.encode('utf-8')).decode('utf-8')

directoryNamePrefix = "_"


class ConnectionContextSimpleFileStorePrivateFns(ObjectStoreConnectionContext):
  objectStore = None
  def __init__(self, objectStore):
    super(ConnectionContextSimpleFileStorePrivateFns, self).__init__()
    self.objectStore = objectStore

  #Return the object type directory or None
  def getObjectTypeDirectory(self, objectType, createObjectDir):
    dirString =  self.objectStore.baseLocation + "/" + directoryNamePrefix + getFileSystemSafeStringFromKey(objectType)
    if self.objectStore.isKnownObjectType(objectType):
      return dirString
    if os.path.exists(dirString):
      return dirString
    if not createObjectDir:
      return None
    os.mkdir(dirString)
    self.objectStore.setKnownObjectType(objectType)
    return dirString

  def getObjectFile(self, objectType, objectKey, createObjectDir):
    dirString = self.getObjectTypeDirectory(objectType, createObjectDir)
    if dirString is None:
      return None
    return dirString + "/" + getFileSystemSafeStringFromKey(objectKey)

  def getObjectJSONWithoutLock(self, objectType, objectKey):
    fileName = self.getObjectFile(objectType, objectKey, False)
    if fileName is None:
      return None, None, None, None

    if not os.path.exists(fileName):
      return None, None, None, None

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
    return filecontentDict["Data"], filecontentDict["ObjVer"], creationDate, lastUpdateDate


class ConnectionContext(ConnectionContextSimpleFileStorePrivateFns):

  #transactional memory not implemented
  def _startTransaction(self):
    pass
  def _commitTransaction(self):
    pass
  def _rollbackTransaction(self):
    pass


  def _saveJSONObject(self, objectType, objectKey, JSONString, objectVersion):
    self.objectStore.fileAccessLock.acquire()
    curTimeValue = self.objectStore.externalFns['getCurDateTime']().isoformat()

    (o_objectDICT, o_ObjectVersion, o_creationDate, o_lastUpdateDate) = self.getObjectJSONWithoutLock(objectType, objectKey)

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

    self.objectStore.fileAccessLock.release()
    return newObjectVersion

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    if not self.objectStore.isKnownObjectType(objectType):
      if ignoreMissingObject:
        return None
      raise TriedToDeleteMissingObjectException
    self.objectStore.fileAccessLock.acquire()

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

    self.objectStore.fileAccessLock.release()
    return None

  #Return value is objectDICT, ObjectVersion, creationDate, lastUpdateDate
  #Return None, None, None, None if object isn't in store
  def _getObjectJSON(self, objectType, objectKey):
    self.objectStore.fileAccessLock.acquire()
    a = self.getObjectJSONWithoutLock(objectType, objectKey)
    self.objectStore.fileAccessLock.release()
    return a


  def _filterFN(self, item, whereClauseText):
    if whereClauseText is None:
      return True
    if whereClauseText == '':
      return True
    ###userDICT = CreateUserObjFromUserDict(appObj, item[0],item[1],item[2],item[3]).getJSONRepresenation()
    #TODO replace with a dict awear generic function
    #  we also need to consider removing spaces from consideration
    return whereClauseText in str(item).upper()

  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    #This is as simple as getting a directory listing
    collectedObjects = {}
    otd = ""
    if self.objectStore.isKnownObjectType(objectType):
      otd = self.getObjectTypeDirectory(objectType, createObjectDir=False)
      if otd is not None:
        objectFiles = localScanDir(otd, False)

        for curFileName in objectFiles:
          #print(otd + "/" + curFileName + "->" + getKeyFromFileSystemSafeString(curFileName))
          objectKey = getKeyFromFileSystemSafeString(curFileName)
          #print("OK:" + objectKey)
          (objectDICT, ObjectVersion, creationDate, lastUpdateDate) = self._getObjectJSON(objectType, objectKey)
          collectedObjects[objectKey] = (objectDICT, ObjectVersion, creationDate, lastUpdateDate)

    ##print('objectStoresMemory._getPaginatedResult self.objectType.objectData[objectType]:', self.objectType.objectData[objectType])
    return self.objectStore.externalFns['getPaginatedResult'](
      collectedObjects,
      outputFN,
      artificalRequestWithPaginationArgs(paginatedParamValues),
      self._filterFN
    )

# Class that will store objects as directoryies and files in the file system
class ObjectStore_SimpleFileStore(ObjectStore):
  baseLocation = ""
  fileAccessLock = None #Make sure we do one operation at a time
  knownExistingObjectTypes = None #reduce os.dir exist calls by storeing objects we know have a dir

  def isKnownObjectType(self, objectType):
    return objectType in self.knownExistingObjectTypes
  def setKnownObjectType(self, objectType):
    self.knownExistingObjectTypes[objectType] = True

  def __init__(self, ConfigDict, externalFns):
    super(ObjectStore_SimpleFileStore, self).__init__(externalFns)

    self.fileAccessLock = threading.Lock()
    self.knownExistingObjectTypes = {}

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

  def _INT_getDictForObjectType(self, objectType):
    if objectType not in self.objectData:
      #print("Creating dict for " + objectType)
      self.objectData[objectType] = dict()
    return self.objectData[objectType]

  #required for unit testing
  # using transaction even though they are not supported
  def _resetDataForTest(self):
    def someFn(connectionContext):
      subDirsToDel = localScanDir(directoryToScan=self.baseLocation, OnlyReturnDirectories=True)

      for curDir in subDirsToDel:
        if curDir.startswith(directoryNamePrefix):
          shutil.rmtree(self.baseLocation + "/" + curDir)

    self.fileAccessLock.acquire()
    self.knownExistingObjectTypes = {}
    # print("SFS 279: reseting all known types")
    self.executeInsideTransaction(someFn)
    self.fileAccessLock.release()

  def _getConnectionContext(self):
    return ConnectionContext(self)