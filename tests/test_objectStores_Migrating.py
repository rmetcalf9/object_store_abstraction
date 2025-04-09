import TestHelperSuperClass

import object_store_abstraction as undertest
import copy
import datetime
import pytz


import test_objectStores_GenericTests as genericTests
import test_objectStores_GenericTests_DoubleStringIndex as genericTestsDoubleStringIndex

ConfigDict = {
  "Type": "Migrating",
  "From": None,
  "To": None
}

MemoryStoreConfigDict = None

migratingStoresStartingAmounts = {
  "MigrationTestObj1": 5,
  "MigrationTestObj2": 3,
  "MigrationTestObj3": 10
}

#@TestHelperSuperClass.wipd
class test_objectStoresMigrating(TestHelperSuperClass.testHelperSuperClass):
  def setupTwoMemoryStoresReadyForAMigration(self, extfns):
    migratingStore = undertest.ObjectStore_Migrating(copy.deepcopy(ConfigDict), extfns, detailLogging=False, type='testMIG', factoryFn=undertest.createObjectStoreInstance)
    genericTests.addSampleRows(
      migratingStore.fromStore ,
      5,
      bbString='BB',
      offset=0,
      objectType="MigrationTestObj1"
    )
    genericTests.addSampleRows(
      migratingStore.fromStore ,
      3,
      bbString='BB',
      offset=0,
      objectType="MigrationTestObj2"
    )
    genericTests.addSampleRows(
      migratingStore.fromStore ,
      10,
      bbString='BB',
      offset=0,
      objectType="MigrationTestObj3"
    )
    #Assert all the data is in from store but not in to store
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.fromStore, copy.deepcopy(migratingStoresStartingAmounts))
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.toStore, {})

    #When querying migrating store the data must be
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore, copy.deepcopy(migratingStoresStartingAmounts))


    return migratingStore
  def assertObjectStoreHasRightNumberOfObjects(self, store, objectCountDict, msgprefix=""):
    def fn(context):
      objectTypesInStore = context.list_all_objectTypes()

      for objectType in objectCountDict:
        if objectType not in objectTypesInStore:
          self.assertFalse(True, msg=msgprefix + "Object type " + objectType + " expected in store but not found")

      for objectType in objectTypesInStore:
        numObjectsOfType = len(context.getAllRowsForObjectType(objectType, None, None, None))
        expectedNumOfObjects = 0
        if objectType in objectCountDict:
          expectedNumOfObjects = objectCountDict[objectType]
        self.assertEqual(numObjectsOfType, expectedNumOfObjects, msg=msgprefix + "Wrong number of " + objectType + " found in store")

    return store.executeInsideConnectionContext(fn)

  def test_genericTests(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Migrating(copy.deepcopy(ConfigDict), self.getObjectStoreExternalFns(), detailLogging=False, type='testMIG', factoryFn=undertest.createObjectStoreInstance)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict, expectPersistance=False)

  def test_genericTests_doublestringindex(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Migrating(copy.deepcopy(ConfigDict), self.getObjectStoreExternalFns(), detailLogging=False, type='testMIG', factoryFn=undertest.createObjectStoreInstance)
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, ConfigDict)


  def test_readingAllRowsDosentMigrate(self):
    #Execute a simple migration between two memory stores.
    migratingStore = self.setupTwoMemoryStoresReadyForAMigration(self.getObjectStoreExternalFns())

    def fn(context):
      for objectType in migratingStoresStartingAmounts:
        allData = None
        allData = context.getAllRowsForObjectType(objectType, None, None, None)
    migratingStore.executeInsideConnectionContext(fn)

    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.fromStore, copy.deepcopy(migratingStoresStartingAmounts))
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.toStore, {})
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore, copy.deepcopy(migratingStoresStartingAmounts))

  def test_fullSimpleMigrationByVisitingAllRecords(self):
    #Execute a simple migration between two memory stores.
    migratingStore = self.setupTwoMemoryStoresReadyForAMigration(self.getObjectStoreExternalFns())

    def fn(context):
      for objectType in migratingStoresStartingAmounts:
        allData = None
        allData = context.getAllRowsForObjectType(objectType, None, None, None)
        for curJSON, objectVersion, _, _, objectKey in allData:
          context.saveJSONObject(objectType, objectKey, curJSON, objectVersion)
    migratingStore.executeInsideTransaction(fn)

    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.fromStore, copy.deepcopy(migratingStoresStartingAmounts))
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.toStore, copy.deepcopy(migratingStoresStartingAmounts))
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore, copy.deepcopy(migratingStoresStartingAmounts))

  def test_onlyChangeTwoRecords(self):
    migratingStore = self.setupTwoMemoryStoresReadyForAMigration(self.getObjectStoreExternalFns())

    changedVal = {"ww": 2134, "tt": "dsaffds"}

    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "123" + str(2)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey = context.getObjectJSON(objectType, objectKey)
      context.saveJSONObject(objectType, objectKey, changedVal, ObjectVersion)

      objectType = "MigrationTestObj2"
      objectKey = "123" + str(1)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey = context.getObjectJSON(objectType, objectKey)
      context.saveJSONObject(objectType, objectKey, changedVal, ObjectVersion)

    migratingStore.executeInsideTransaction(fn)

    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.fromStore, copy.deepcopy(migratingStoresStartingAmounts))
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.toStore, {
      "MigrationTestObj1": 1,
      "MigrationTestObj2": 1
    })
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore, copy.deepcopy(migratingStoresStartingAmounts))

    #Make sure data is correct in migratingStore
    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "123" + str(2)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, changedVal, msg="Incorrect data in migrated store")

      objectType = "MigrationTestObj2"
      objectKey = "123" + str(1)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, changedVal, msg="Incorrect data in migrated store")
    migratingStore.executeInsideConnectionContext(fn)

    #Make sure data is correct in toStore
    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "123" + str(2)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, changedVal, msg="Incorrect data in tostore store")

      objectType = "MigrationTestObj2"
      objectKey = "123" + str(1)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, changedVal, msg="Incorrect data in tostore store")
    migratingStore.toStore.executeInsideConnectionContext(fn)

  def test_onlyCreateTwoRecords(self):
    migratingStore = self.setupTwoMemoryStoresReadyForAMigration(self.getObjectStoreExternalFns())

    newVal = {"ww": 2134, "tt": "dsaffds"}

    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "nn123" + str(2)
      context.saveJSONObject(objectType, objectKey, newVal, objectVersion=None)

      objectType = "MigrationTestObj2"
      objectKey = "nn123" + str(1)
      context.saveJSONObject(objectType, objectKey, newVal, objectVersion=None)

    migratingStore.executeInsideTransaction(fn)

    expTot = copy.deepcopy(migratingStoresStartingAmounts)
    expTot["MigrationTestObj1"] = expTot["MigrationTestObj1"] + 1
    expTot["MigrationTestObj2"] = expTot["MigrationTestObj2"] + 1
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.fromStore, expTot)
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.toStore, {
      "MigrationTestObj1": 1,
      "MigrationTestObj2": 1
    })
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore, copy.deepcopy(expTot), "MigCheck:")

    #Make sure data is correct in migratingStore
    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "nn123" + str(2)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, newVal, msg="Incorrect data in migrated store")

      objectType = "MigrationTestObj2"
      objectKey = "nn123" + str(1)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, newVal, msg="Incorrect data in migrated store")
    migratingStore.executeInsideConnectionContext(fn)

    #Make sure data is correct in toStore
    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "nn123" + str(2)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, newVal, msg="Incorrect data in tostore store")

      objectType = "MigrationTestObj2"
      objectKey = "nn123" + str(1)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      self.assertEqual(objectDICT, newVal, msg="Incorrect data in tostore store")
    migratingStore.toStore.executeInsideConnectionContext(fn)

  def test_removeRecord(self):
    #The records are never added to the to store
    migratingStore = self.setupTwoMemoryStoresReadyForAMigration(self.getObjectStoreExternalFns())

    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "123" + str(2)
      _, ObjectVersion, _, _, _ = context.getObjectJSON(objectType, objectKey)
      context.removeJSONObject(objectType, objectKey, ObjectVersion)

      objectType = "MigrationTestObj2"
      objectKey = "123" + str(1)
      _, ObjectVersion, _, _, _ = context.getObjectJSON(objectType, objectKey)
      context.removeJSONObject(objectType, objectKey, ObjectVersion)

    migratingStore.executeInsideTransaction(fn)

    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.toStore, {
    }, "Checking toStore after removal: ")


    expTot = copy.deepcopy(migratingStoresStartingAmounts)
    expTot["MigrationTestObj1"] = expTot["MigrationTestObj1"] - 1
    expTot["MigrationTestObj2"] = expTot["MigrationTestObj2"] - 1
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore.fromStore, expTot, "Checking from store after removal: ")
    self.assertObjectStoreHasRightNumberOfObjects(migratingStore, copy.deepcopy(expTot), "Checking migrated store after removal: ")

    #Make sure records are missing in migratingStore
    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "123" + str(2)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      if objectDICT is not None:
        self.assertFalse(True, msg="Object was not remove from migrated store")

      objectType = "MigrationTestObj2"
      objectKey = "123" + str(1)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      if objectDICT is not None:
        self.assertFalse(True, msg="Object was not remove from migrated store")
    migratingStore.executeInsideConnectionContext(fn)

    #Make sure records are missing in toStore
    def fn(context):
      objectType = "MigrationTestObj1"
      objectKey = "123" + str(2)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      if objectDICT is not None:
        self.assertFalse(True, msg="Object was not remove from tostore store")

      objectType = "MigrationTestObj2"
      objectKey = "123" + str(1)
      objectDICT, ObjectVersion, creationDate, lastUpdateDate, _ = context.getObjectJSON(objectType, objectKey)
      if objectDICT is not None:
        self.assertFalse(True, msg="Object was not remove from tostore store")
    migratingStore.toStore.executeInsideConnectionContext(fn)

#test_removalAfterFullMigration
