import TestHelperSuperClass
import test_objectStores_GenericTests as genericTests
import object_store_abstraction as undertest
import copy
import time

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
    "bad": {
      "cache": True,
      "maxCacheSize": 100,
      "cullToSize": 50,
      "timeout": 1000 #in miliseconds 1000 = 1 second
    }
  }
}

class helper(TestHelperSuperClass.testHelperSuperClass):
  pass

@TestHelperSuperClass.wipd
class test_objectStoresMigrating(helper):
  def test_genericTests(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Caching(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict, expectPersistance=False)

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

    self.assertEquals(len(cacheRows), 100)
    self.assertEquals(len(mainRows), 500)

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

  def test_cacheItemExpires(self):
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

    #Load it into cache
    def dbfn(storeConnection):
      return storeConnection.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )
    (objectDict, ObjectVersion, creationDate, lastUpdateDate,
     objectKey) = undertestCacheObjectStore.executeInsideConnectionContext(dbfn)

    time.sleep(1.1)

    def dbfn(storeConnection):
      return storeConnection.getObjectJSON(
        objectType=objectType,
        objectKey=objectKey
      )
    (objectDict, ObjectVersion, creationDate, lastUpdateDate,
     objectKey) = undertestCacheObjectStore.executeInsideConnectionContext(dbfn)

