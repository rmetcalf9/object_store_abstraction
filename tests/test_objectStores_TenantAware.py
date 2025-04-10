import TestHelperSuperClass

import object_store_abstraction as undertest

from test_objectStores_GenericTests import JSONString, JSONString2, assertCorrectPaginationResult
import copy
import json
from python_Testing_Utilities import objectsEqual
import test_objectStores_GenericTests_DoubleStringIndex as genericTestsDoubleStringIndex


ConfigDict = {}

testTenant1 = "testTenant1"
testTenant2 = "testTenant2"

tenant1ObjectsToUse = ["objT1","objT2","objT3","objT4"]
tenant2ObjectsToUse = ["objT2","objT3","objT4", "objT5"]

def generateExpectedResultList(tenantName, baseJSON):
  expectedRes = []
  for x in range(0,10):
    expectedJSON = getAlteredJSONString(x, baseJSON)
    expectedRes.append(expectedJSON)
  expectedJSON = getAlteredJSONString("123" + tenantName, baseJSON)
  expectedRes.append(expectedJSON)
  return expectedRes


def getAlteredJSONString(x, srcJSON):
  res = copy.deepcopy(srcJSON)
  res['AA'] = x
  res['BB'] = "bbStringFN(x)"
  return res

class local_helpers(TestHelperSuperClass.testHelperSuperClass):
  def generateSimpleMemoryObjectStore(self):
    memStore = undertest.ObjectStore_Memory(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    return undertest.ObjectStore_TenantAware(memStore)

  def test_genericTests_doublestringindex(self):
    def getObjFn(ConfigDict, resetData = True):
      mem_object_store = undertest.ObjectStore_Memory(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
      return undertest.ObjectStore_TenantAware(objectStore=mem_object_store)
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, ConfigDict, tenantAweare=True)


  def setupSomeTestDataForObjectType(self, connectionContext, tenantName, objectType, baseStr):
    for x in range(0,10):
      toInsert = getAlteredJSONString(x, baseStr)
      xres = connectionContext.saveJSONObject(objectType, "123" + str(x), toInsert, None)
    #Add object with special tenant spercific key
    toInsert = getAlteredJSONString("123" + tenantName, baseStr)
    xres = connectionContext.saveJSONObject(objectType, "123" + tenantName, toInsert, None)

  def assertObjectTypeDataCorrectViaGETOnly(self, objectStore, tenantName, objectType, baseStr, shouldBePresent=True):
    #Assume that data has been input using setupSomeTestDataForObjectType
    def someFn(connectionContext):
      for x in range(0,10):
        objectKey = "123" + str(x)
        expectedJSON = getAlteredJSONString(x, baseStr)
        expectedJSON['BB'] = "bbStringFN(x)"
        (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = connectionContext.getObjectJSON(objectType, objectKey)
        if shouldBePresent:
          if objectDICT is None:
            self.assertTrue(False,msg="Item should be in store but is not")
          self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, expectedJSON, [  ], msg='Saved object dosen\'t match for key ' + objectKey)
        else:
          self.assertEqual(objectDICT, None)
      objectKey = "123" + tenantName
      expectedJSON = getAlteredJSONString("123" + tenantName, baseStr)
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


  def setupUnevenData(self):
    objectStoreType = self.generateSimpleMemoryObjectStore()
    self.setupSomeTestData(objectStoreType, testTenant1, JSONString, tenant1ObjectsToUse)
    self.setupSomeTestData(objectStoreType, testTenant2, JSONString2, tenant2ObjectsToUse)
    return objectStoreType

#@TestHelperSuperClass.wipd
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
    objectStoreType = self.setupUnevenData()

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

  def test_getAllRowsForObjectType(self):
    objectStoreType = self.setupUnevenData()

    def dbfn1(storeConnection):
      def outputFN(item):
        return item[0]
      filterFN = None

      resALL = storeConnection.getAllRowsForObjectType("objT1", filterFN, outputFN, None)

      expectedRes = generateExpectedResultList(testTenant1, JSONString)

      x = 0
      actualResSorted = []
      while x < 11:
        actualResSorted.append(resALL[x])
        x = x + 1

      self.assertEqual(actualResSorted, expectedRes)

      resALL = storeConnection.getAllRowsForObjectType("objT2", filterFN, outputFN, None)

      expectedRes = generateExpectedResultList(testTenant1, JSONString)

      x = 0
      actualResSorted = []
      while x < 11:
        actualResSorted.append(resALL[x])
        x = x + 1

      self.assertEqual(actualResSorted, expectedRes)

      resALL = storeConnection.getAllRowsForObjectType("objT5", filterFN, outputFN, None)

      expectedRes = []
      x = 0
      actualResSorted = []
      while x < 0:
        actualResSorted.append(resALL[x])
        x = x + 1

      self.assertEqual(actualResSorted, expectedRes)
    objectStoreType.executeInsideConnectionContext(testTenant1, dbfn1)

    def dbfn2(storeConnection):
      def outputFN(item):
        return item[0]
      filterFN = None

      resALL = storeConnection.getAllRowsForObjectType("objT1", filterFN, outputFN, None)

      expectedRes = []
      x = 0
      actualResSorted = []
      while x < 0:
        actualResSorted.append(resALL[x])
        x = x + 1

      self.assertEqual(actualResSorted, expectedRes)

      resALL = storeConnection.getAllRowsForObjectType("objT2", filterFN, outputFN, None)

      expectedRes = generateExpectedResultList(testTenant2, JSONString2)

      x = 0
      actualResSorted = []
      while x < 11:
        actualResSorted.append(resALL[x])
        x = x + 1
      self.assertEqual(actualResSorted, expectedRes)


      resALL = storeConnection.getAllRowsForObjectType("objT5", filterFN, outputFN, None)

      expectedRes = generateExpectedResultList(testTenant2, JSONString2)

      x = 0
      actualResSorted = []
      while x < 11:
        actualResSorted.append(resALL[x])
        x = x + 1
      self.assertEqual(actualResSorted, expectedRes)

    objectStoreType.executeInsideConnectionContext(testTenant2, dbfn2)

  def test_removeJSONObject(self):
    objectStoreType = self.setupUnevenData()
    objectType = "objT3"
    objectKey = "123" + str(3)

    def someFn1(storeConnection):
      #Remove object that is in both Tenant1 and testTenant2 from Tenant1
      newVer = storeConnection.removeJSONObject(objectType, objectKey, 1, False)

      #Check object is not in Tenant1
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, _) = storeConnection.getObjectJSON(objectType, objectKey)
      if objectDICT is not None:
        self.assertTrue(False, msg="Object not deleted from Tenant1")

    objectStoreType.executeInsideTransaction(testTenant1, someFn1)

    def someFn2(storeConnection):
      #Check object is still in tenant2
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, _) = storeConnection.getObjectJSON(objectType, objectKey)
      if objectDICT is None:
        self.assertTrue(False, msg="Object was deleted from Tenant2")
    objectStoreType.executeInsideConnectionContext(testTenant2, someFn2)

  def test_updateJSONObject(self):
    objectStoreType = self.setupUnevenData()
    objectType = "objT4"
    objectKey = "123" + str(5)

    newObjJSON = copy.deepcopy(JSONString)
    newObjJSON['BB'] = "something has been changed!!!"

    def someFn1(storeConnection):
      #Get object in tenant1 to get objectversion
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, _) = storeConnection.getObjectJSON(objectType, objectKey)

      #Update object in Tenant1
      def updateFn(obj, connectionContext):
        self.assertJSONStringsEqualWithIgnoredKeys(obj, getAlteredJSONString(5, JSONString), [  ], msg='inside updateFN passed obj dosen\'t match')
        return newObjJSON
      newVer = storeConnection.updateJSONObject(objectType, objectKey, updateFn, ObjectVersion)

      #Make sure object is updated in Tenant1
      (objectDICT2, _, creationDate, lastUpdateDate, _) = storeConnection.getObjectJSON(objectType, objectKey)
      self.assertJSONStringsEqualWithIgnoredKeys(objectDICT2, copy.deepcopy(newObjJSON), [  ], msg='object was not updated')

    objectStoreType.executeInsideTransaction(testTenant1, someFn1)

    def someFn2(storeConnection):
      #Make sure object has origional value in Tenant2
      (objectDICT3, _, creationDate, lastUpdateDate, _) = storeConnection.getObjectJSON(objectType, objectKey)
      self.assertJSONStringsEqualWithIgnoredKeys(objectDICT3, getAlteredJSONString(5, JSONString2), [  ], msg='object was updated in wrong tenant')
    objectStoreType.executeInsideConnectionContext(testTenant2, someFn2)

  def test_getPaginatedResult(self):
    objectStoreType = self.setupUnevenData()
    objectType = "objT3"
    objectType2 = "objT1"

    #assert Paginated result from Tenant1 is correct for objectType in both (not double results)
    def dbfn(storeConnection):
      def outputFN(item):
        return item[0]
      paginatedParamValues = {
        'offset': 0,
        'pagesize': 20,
        'query': '',
        'sort': None
      }
      res = storeConnection.getPaginatedResult(objectType, paginatedParamValues, outputFN)
      expectedRes = generateExpectedResultList(testTenant1, JSONString)
      assertCorrectPaginationResult(self, res, 0, 20, 11)

      a = list(map(lambda x: json.dumps(x), res['result']))
      b = list(map(lambda x: json.dumps(x), expectedRes))
      self.assertTrue(objectsEqual(a, b), msg="Wrong result")

    objectStoreType.executeInsideConnectionContext(testTenant1, dbfn)

    #TODO assert Paginated result from Tenant2 is correct for objectType in tenant1 only objT1 (should get 0 rows)
    def dbfn2(storeConnection):
      def outputFN(item):
        return item[0]
      paginatedParamValues = {
        'offset': 0,
        'pagesize': 20,
        'query': '',
        'sort': None
      }
      res = storeConnection.getPaginatedResult(objectType2, paginatedParamValues, outputFN)
      expectedRes = []
      assertCorrectPaginationResult(self, res, 0, 20, 0)

      a = list(map(lambda x: json.dumps(x), res['result']))
      b = list(map(lambda x: json.dumps(x), expectedRes))
      self.assertTrue(objectsEqual(a, b), msg="Wrong result")

    objectStoreType.executeInsideConnectionContext(testTenant2, dbfn2)

  def test_list_all_objectTypes(self):
    objectStoreType = self.setupUnevenData()

    def getObjTpeLis(context):
      return context.list_all_objectTypes()
    t1ObjectLis = objectStoreType.executeInsideConnectionContext(testTenant1, getObjTpeLis)
    t2ObjectLis = objectStoreType.executeInsideConnectionContext(testTenant2, getObjTpeLis)

    self.assertTrue(objectsEqual(t1ObjectLis, tenant1ObjectsToUse), msg="Wrong object list supplied for Tenant1 got - " + str(t1ObjectLis))
    self.assertTrue(objectsEqual(t2ObjectLis, tenant2ObjectsToUse), msg="Wrong object list supplied for Tenant2 got - " + str(t2ObjectLis))
