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

  def setupSomeTestDataForObjectType(self, connectionContext, tenantName, objectType, baseStr):
    for x in range(0,10):
      toInsert = copy.deepcopy(baseStr)
      toInsert['AA'] = x
      toInsert['BB'] = "bbStringFN(x)"
      xres = connectionContext.saveJSONObject(objectType, "123" + str(x), toInsert, None)
    #Add object with special tenant spercific key
    toInsert = copy.deepcopy(baseStr)
    toInsert['AA'] = "123" + tenantName
    toInsert['BB'] = "bbStringFN(x)"
    xres = connectionContext.saveJSONObject(objectType, "123" + tenantName, toInsert, None)

  def assertObjectTypeDataCorrectViaGETOnly(self, objectStore, tenantName, objectType, baseStr, shouldBePresent=True):
    #Assume that data has been input using setupSomeTestDataForObjectType
    def someFn(connectionContext):
      for x in range(0,10):
        objectKey = "123" + str(x)
        expectedJSON = copy.deepcopy(baseStr)
        expectedJSON['AA'] = x
        expectedJSON['BB'] = "bbStringFN(x)"
        (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = connectionContext.getObjectJSON(objectType, objectKey)
        if shouldBePresent:
          if objectDICT is None:
            self.assertTrue(False,msg="Item should be in store but is not")
          self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, expectedJSON, [  ], msg='Saved object dosen\'t match for key ' + objectKey)
        else:
          self.assertEqual(objectDICT, None)
      objectKey = "123" + tenantName
      expectedJSON = copy.deepcopy(baseStr)
      expectedJSON['AA'] = "123" + tenantName
      expectedJSON['BB'] = "bbStringFN(x)"
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = connectionContext.getObjectJSON(objectType, objectKey)
      if shouldBePresent:
        if objectDICT is None:
          self.assertTrue(False,msg="Item should be in store but is not")
        self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, expectedJSON, [  ], msg='Saved object dosen\'t match for key ' + objectKey)
      else:
        self.assertEqual(objectDICT, None)
    objectStore.executeInsideTransaction(tenantName, someFn)


  def setupSomeTestData(self, objectStore, tenantName, baseStr, objTypeLis):
    def someFn(connectionContext):
      for x in objTypeLis:
        self.setupSomeTestDataForObjectType(connectionContext, tenantName, x, baseStr)
    objectStore.executeInsideTransaction(tenantName, someFn)


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

  def test_writenDataDifferentBetweenTenantsEvenWithSameObjectKey(self):
    objectStoreType = self.generateSimpleMemoryObjectStore()
    self.setupSomeTestData(objectStoreType, testTenant1, JSONString, ["objT1","objT2","objT3","objT4"])
    self.setupSomeTestData(objectStoreType, testTenant2, JSONString2, ["objT2","objT3","objT4", "objT5"])

    #Check objT1 correct in testTenant1
    self.assertObjectTypeDataCorrectViaGETOnly(objectStoreType, testTenant1, "objT1", JSONString)
    #Check objT1 not in testTenant2
    self.assertObjectTypeDataCorrectViaGETOnly(objectStoreType, testTenant2, "objT1", JSONString2, shouldBePresent=False)

    #Check correct value for objT2 in testTenant1
    self.assertObjectTypeDataCorrectViaGETOnly(objectStoreType, testTenant1, "objT2", JSONString)
    #Check correct value for objT2 in testTenant2
    self.assertObjectTypeDataCorrectViaGETOnly(objectStoreType, testTenant2, "objT2", JSONString2)

    #Check objT5 not in testTenant1
    self.assertObjectTypeDataCorrectViaGETOnly(objectStoreType, testTenant1, "objT5", JSONString, shouldBePresent=False)
    #Check objT5 correct in testTenant2
    self.assertObjectTypeDataCorrectViaGETOnly(objectStoreType, testTenant2, "objT5", JSONString2)
