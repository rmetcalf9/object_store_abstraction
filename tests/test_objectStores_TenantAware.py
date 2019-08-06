from TestHelperSuperClass import testHelperSuperClass, wipd

import object_store_abstraction as undertest

from test_objectStores_GenericTests import JSONString, JSONString2
import copy

ConfigDict = {}

testTenant1 = "testTenant1"
testTenant2 = "testTenant2"

class local_helpers(testHelperSuperClass):
  def generateSimpleMemoryObjectStore(self):
    memStore = undertest.ObjectStore_Memory(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    return undertest.ObjectStore_TenantAware(memStore)

@wipd
class test_objectStoresTenantAware(local_helpers):

  def test_basicTest(self):
    objectStoreType = self.generateSimpleMemoryObjectStore()

    def dbfn(storeConnection):
      def someFn(connectionContext):
        savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), None)
      savedVer = storeConnection.executeInsideTransaction(someFn)

      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
      self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, copy.deepcopy(JSONString), [  ], msg='Saved object dosen\'t match')
      self.assertJSONStringsEqualWithIgnoredKeys(objectKey, "123", [  ], msg='Returned key wrong')
    objectStoreType.executeInsideConnectionContext(testTenant1, dbfn)
