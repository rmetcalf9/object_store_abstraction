import TestHelperSuperClass
import test_objectStores_GenericTests as genericTests
import object_store_abstraction as undertest
import copy
import time
from unittest import mock
import test_objectStores_GenericTests_DoubleStringIndex as genericTestsDoubleStringIndex


ConfigDict = {
  "Type": "Caching",
  ##"Caching": {**}, #If missing memory is used
  "Main": {
    "Type": "Memory" #For testing main store is memory
     #in real uses it would not be
  }, #Main persistant store
  "DefaultPolicy": {
    "cache": True,
    "maxCacheSize": 100,
    "cullToSize": 50,
    "timeout": 1000 #in miliseconds 1000 = 1 second
  },
  "ObjectTypeOverride": {
    "cacheOff": {
      "cache": False,
      "maxCacheSize": 100,
      "cullToSize": 50,
      "timeout": 1000 #in miliseconds 1000 = 1 second
    }
  }
}

class helper(TestHelperSuperClass.testHelperSuperClass):
  undertestCacheObjectStore=None
  internalMainObjectStore=None
  internalCacheObjectStore=None

  def setUp(self):
    self.undertestCacheObjectStore = undertest.ObjectStore_Caching(
      ConfigDict,
      self.getObjectStoreExternalFns(),
      detailLogging=False, type='testMEM',
      factoryFn=undertest.createObjectStoreInstance
    )
    self.internalMainObjectStore = self.undertestCacheObjectStore.mainStore
    self.internalCacheObjectStore = self.undertestCacheObjectStore.cachingStore

  def addSingleRow(self, store, objectType, objectKey, rowData):
    def dbfn(storeConnection):
      storeConnection.saveJSONObject(objectType, objectKey, rowData, objectVersion=None)
    store.executeInsideTransaction(dbfn)

  def changeSingleRow(self, store, objectType, objectKey, rowData):
    def dbfn(storeConnection):
      ret, newobjver, _, _, _ = storeConnection.getObjectJSON(objectType, objectKey)
      storeConnection.saveJSONObject(objectType, objectKey, rowData, objectVersion=newobjver)
    store.executeInsideTransaction(dbfn)

  #changeSingleRow(self.internalMainObjectStore, objectType, objectKey, rowData=changedData)

  def readSingleRow(self, store, objectType, objectKey, verifyMatches, msg=None):
    def dbfn(storeConnection):
      return storeConnection.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )
    ret, _, _, _, _ = store.executeInsideConnectionContext(dbfn)
    if verifyMatches is not None:
      self.assertEqual(ret, verifyMatches, msg=msg)

  #readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=exampleData)

#@TestHelperSuperClass.wipd
class test_objectStoresMigrating(helper):
  def test_genericTests(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Caching(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict, expectPersistance=False)

  def test_genericTests_doublestringindex(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Caching(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, ConfigDict)

  def test_canLookupObjectInMainStoreNotAddedViaCache(self):
    objectType = "CacheTestObj1"
    undertestCacheObjectStore = undertest.ObjectStore_Caching(
      ConfigDict,
      self.getObjectStoreExternalFns(),
      detailLogging=False, type='testMEM',
      factoryFn=undertest.createObjectStoreInstance
    )
    internalMainObjectStore = undertestCacheObjectStore.mainStore
    genericTests.addSampleRows(
      internalMainObjectStore,
      5,
      bbString='BB',
      offset=0,
      objectType=objectType
    )
    objectKey="1231"
    def dbfn(storeConnection):
      #Get sample row from cacheStore and make sure it is correct
      return storeConnection.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )
    (objectDict, ObjectVersion, creationDate, lastUpdateDate, objectKey) = undertestCacheObjectStore.executeInsideConnectionContext(dbfn)
    expected = copy.deepcopy(genericTests.JSONString)
    expected["AA"] = 1
    self.assertEqual(objectDict, expected)




  #@TestHelperSuperClass.wipd
  def test_secondLookupComesFromCache(self):
    objectType = "CacheTestObj1"
    undertestCacheObjectStore = undertest.ObjectStore_Caching(
      ConfigDict,
      self.getObjectStoreExternalFns(),
      detailLogging=False, type='testMEM',
      factoryFn=undertest.createObjectStoreInstance
    )
    internalMainObjectStore = undertestCacheObjectStore.mainStore
    internalCacheObjectStore = undertestCacheObjectStore.cachingStore

    #Add data to the main store
    internalMainObjectStore = undertestCacheObjectStore.mainStore
    genericTests.addSampleRows(
      internalMainObjectStore,
      5,
      bbString='BB',
      offset=0,
      objectType=objectType
    )
    objectKey="1231"

    #Lookup object from undertest
    objectKey = "1231"

    def dbfn(storeConnection):
      return storeConnection.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )

    (objectDict, ObjectVersion, creationDate, lastUpdateDate,
     objectKey) = undertestCacheObjectStore.executeInsideConnectionContext(dbfn)
    expected = copy.deepcopy(genericTests.JSONString)
    expected["AA"] = 1
    self.assertEqual(objectDict, expected)

    #Check it is in cache
    objectKey = "1231"

    def dbfn(storeConnection):
      # Get sample row from cacheStore and make sure it is correct
      return storeConnection.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )

    (objectDict, ObjectVersion, creationDate, lastUpdateDate,
     objectKey) = internalCacheObjectStore.executeInsideConnectionContext(dbfn)
    expected = copy.deepcopy(genericTests.JSONString)
    expected["AA"] = 1
    self.assertEqual(objectDict["d"], expected)

    #change the cache only
    def changeInCacheFn(connectionContext):
      obj, _, _, _, _ = connectionContext.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )
      obj["d"]["BB"] = "BBCacheChange"
      return connectionContext.saveJSONObject(objectKey, objectType, obj, None)
    savedVer = internalCacheObjectStore.executeInsideTransaction(changeInCacheFn)

    #lookup again make sure we get the change
    def dbfn(storeConnection):
      return storeConnection.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )
    (objectDict, ObjectVersion, creationDate, lastUpdateDate,
     objectKey) = undertestCacheObjectStore.executeInsideConnectionContext(dbfn)
    expected = copy.deepcopy(genericTests.JSONString)
    expected["AA"] = 1
    expected["BB"] = "BBCacheChange"
    self.assertEqual(objectDict, expected)

  #@TestHelperSuperClass.wipd
  def test_cacheWontGrowAboveMaxSizeWhenAddingdifferentKEys(self):
    objectType = "CacheTestObj1"
    undertestCacheObjectStore = undertest.ObjectStore_Caching(
      ConfigDict,
      self.getObjectStoreExternalFns(),
      detailLogging=False, type='testMEM',
      factoryFn=undertest.createObjectStoreInstance
    )
    internalMainObjectStore = undertestCacheObjectStore.mainStore
    internalCacheObjectStore = undertestCacheObjectStore.cachingStore

    genericTests.addSampleRows(
      undertestCacheObjectStore,
      500,
      bbString='BB',
      offset=0,
      objectType=objectType
    )
    objectKey="1231"

    def dbfn(con):
      return con.getAllRowsForObjectType(objectType=objectType, filterFN=None, outputFN=None, whereClauseText="")
    cacheRows = internalCacheObjectStore.executeInsideConnectionContext(dbfn)
    mainRows = internalMainObjectStore.executeInsideConnectionContext(dbfn)

    self.assertEqual(len(cacheRows), 100)
    self.assertEqual(len(mainRows), 500)

  def test_deleteItem(self):
    objectType = "CacheTestObj1"

    undertestCacheObjectStore = undertest.ObjectStore_Caching(
      ConfigDict,
      self.getObjectStoreExternalFns(),
      detailLogging=False, type='testMEM',
      factoryFn=undertest.createObjectStoreInstance
    )
    internalMainObjectStore = undertestCacheObjectStore.mainStore
    internalCacheObjectStore = undertestCacheObjectStore.cachingStore

    genericTests.addSampleRows(
      undertestCacheObjectStore,
      500,
      bbString='BB',
      offset=0,
      objectType=objectType
    )
    objectKey="1231"

    def dbfn(connectionContext):
      return connectionContext.removeJSONObject(objectType=objectType, objectKey=objectKey, objectVersion=1, ignoreMissingObject=False)
    newObjectVersion = undertestCacheObjectStore.executeInsideTransaction(dbfn)

  def test_cacheNotUsedWhenItIsConfiguredOff(self):
    objectType = "cacheOff"
    objectKey="1231"
    exampleData = copy.deepcopy(genericTests.JSONString)
    changedData = copy.deepcopy(exampleData)
    changedData["BB"] = "BBChanged"

    self.addSingleRow(self.undertestCacheObjectStore, objectType, objectKey, rowData=exampleData)
    self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=exampleData)

    #Make a change to the underlying MAIN data
    self.changeSingleRow(self.internalMainObjectStore, objectType, objectKey, rowData=changedData)

    self.readSingleRow(self.internalMainObjectStore, objectType, objectKey, verifyMatches=changedData)

    #Changed value should show up immedatadly
    self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=changedData)

  def test_cacheItemExpires(self):
    objectType = "CacheTestObj1"
    objectKey="1231"
    exampleData = copy.deepcopy(genericTests.JSONString)
    changedData = copy.deepcopy(exampleData)
    changedData["BB"] = "BBChanged"

    self.addSingleRow(self.undertestCacheObjectStore, objectType, objectKey, rowData=exampleData)
    self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=exampleData)

    #Make a change to the underlying MAIN data
    self.changeSingleRow(self.internalMainObjectStore, objectType, objectKey, rowData=changedData)

    self.readSingleRow(self.internalMainObjectStore, objectType, objectKey, verifyMatches=changedData)

    #Still should be origional value in main
    self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=exampleData)

    time.sleep(1.1)

    #now should have changed since record expires after one second
    self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=changedData)

  def test_cullQueueNotContainigDuplicateIDsResultsInManyCacheHits(self):
    objectType = "CacheTestObj1"
    objectKey="1231"
    exampleData = copy.deepcopy(genericTests.JSONString)
    changedData = copy.deepcopy(exampleData)
    changedData["BB"] = "BBChanged"

    #Make sure the cullqueue is full (Config max queue size to 50)
    self.addSingleRow(self.undertestCacheObjectStore, objectType, objectKey, rowData=exampleData)
    curTime = time.perf_counter()
    for x in range(1,150):
      curTime += 1 + (ConfigDict["DefaultPolicy"]["timeout"] / 1000)
      with mock.patch('time.perf_counter', return_value=curTime) as stompConnection_function:
        self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=exampleData)

    #Wait a bit
    curTime += 1 + (ConfigDict["DefaultPolicy"]["timeout"] / 1000)
    curTime += 1 + (ConfigDict["DefaultPolicy"]["timeout"] / 1000)
    #Query row once and make sure we still only use the cache ONCE!
    with mock.patch('time.perf_counter', return_value=curTime) as stompConnection_function:
      self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=exampleData)
      #Change in main store (should be ignored as record is in cache!)
      self.changeSingleRow(self.internalMainObjectStore, objectType, objectKey, rowData=changedData)
      self.readSingleRow(self.undertestCacheObjectStore, objectType, objectKey, verifyMatches=exampleData, msg="Did not get expected cache HIT!")

