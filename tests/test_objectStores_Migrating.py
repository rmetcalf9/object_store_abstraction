from TestHelperSuperClass import testHelperSuperClass, wipd

import object_store_abstraction as undertest
import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests

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


class test_objectStoresMigrating(testHelperSuperClass):
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
  def assertObjectStoreHasRightNumberOfObjects(self, store, objectCountDict):
    def fn(context):
      objectTypesInStore = context.list_all_objectTypes()

      for objectType in objectCountDict:
        if objectType not in objectTypesInStore:
          self.assertFalse(True, msg="Object type " + objectType + " expected in store but not found")

      for objectType in objectTypesInStore:
        if objectType not in objectCountDict:
          self.assertFalse(True, msg="Object type " + objectType + " found in store but not expected")
        numObjectsOfType = len(context.getAllRowsForObjectType(objectType, None, None, None))
        self.assertEqual(numObjectsOfType, objectCountDict[objectType], msg="Wrong number of " + objectType + " found in store")

      print("OTIS:",len(objectTypesInStore))

    return store.executeInsideConnectionContext(fn)

  def test_genericTests(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Migrating(copy.deepcopy(ConfigDict), self.getObjectStoreExternalFns(), detailLogging=False, type='testMIG', factoryFn=undertest.createObjectStoreInstance)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict, expectPersistance=False)

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

  @wipd
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



#  def test_onlyChangeTwoRecords(self):

#  def test_onlyCreateTwoRecords(self):

#  def test_onlyRemoveRecords(self):
