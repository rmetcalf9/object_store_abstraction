#test_objectStores_GenericTests
# this file includes generic test base code for object stores
# so they don't need to be implemented in individual test classes.



import datetime
import pytz
import copy
from python_Testing_Utilities import objectsEqual
import json

#import exceptions
from object_store_abstraction import WrongObjectVersionException, SuppliedObjectVersionWhenCreatingException, MissingTransactionContextException, UnallowedMutationException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException

JSONString = {
  'AA': "AA",
  'BB': "BB",
  "CC": {
    "CC.AA": "AA",
    "CC.BB": "BB",
    "CC.CC": "CC"
  }
  #My test comparison can't handle bytes
  #'exampleByteObject': b'abc',
  #'exampleListObject': [1, 2, 3],
  #'exampleListObjectWithSubObject': [{'a':'a', 'bytes': b'b'}, {'bytes': b'b'}],
  #'listOfBytes': [b'abc1', b'abc2']
}
JSONString2 = {
  'AA': "AA2",
  'BB': "BB2",
  "CC": {
    "CC.AA": "AA2",
    "CC.BB": "BB2",
    "CC.CC": "CC2"
  }
  #My test comparison can't handle bytes
  #'exampleByteObject': b'abc2',
  #'exampleListObject': [12, 22, 32],
  #'exampleListObjectWithSubObject': [{'a':'a2', 'bytes': b'b2'}, {'bytes': b'b2'}],
  #'listOfBytes': [b'abc1', b'abc2']
}

persistanceTestList = ["tt_listAllObjectTypes_MutipleTypesSaveAndLoad"]


def isThisTestToRun(nam, expectPersistance, reqObjCon):
  ##print("nam:", nam, ":", expectPersistance)
  if nam in persistanceTestList:
    if not expectPersistance:
      return False
  if nam.startswith("t_"):
    return not reqObjCon
  if nam.startswith("tt_"):
    return reqObjCon
  return False

def runAllGenericTests(testClass, getObjFn, ConfigDict, expectPersistance=True):
  curModuleName = globals()['__name__']


  #globalsCopy = copy.deepcopy(globals())
  globalsCopy = []
  testsRequiringObjConsturctor = []
  for x in globals():
    if isThisTestToRun(x, expectPersistance, False):
      globalsCopy.append(x)
    elif isThisTestToRun(x, expectPersistance, True):
      testsRequiringObjConsturctor.append(x)
  for x in globalsCopy:
      #print("**********************************************************************")
      #print("    test " + x)
      #print("**********************************************************************")
      test_fn = globals()[x]
      obj = getObjFn(ConfigDict)
      test_fn(testClass, obj)
      #print("")
  for x in testsRequiringObjConsturctor:
      test_fn = globals()[x]
      test_fn(testClass, getObjFn, ConfigDict)

#*************************************
#   SaveJSONObject Tests
#*************************************


def t_saveFailsWithInvalidObjectVersionFirstSave(testClass, objectStoreType):
  objVerIDToSaveAs = 123

  def someFn(connectionContext):
    ##Taken out of assertRaises so I can see stack trace of other exception
    #savedVer = connectionContext.saveJSONObject("Test", "123", JSONString, objVerIDToSaveAs)
    with testClass.assertRaises(Exception) as context:
      savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), objVerIDToSaveAs)
    testClass.checkGotRightException(context,SuppliedObjectVersionWhenCreatingException)
  objectStoreType.executeInsideTransaction(someFn)

def t_saveFailsWithInvalidObjectVersionSecondSave(testClass, objectStoreType):
  objVerIDToSaveAs = 123

  def dbfn(storeConnection):
    def someFn(connectionContext):
      return storeConnection.saveJSONObject("Test", "123", copy.deepcopy(JSONString), None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    def someFn2(connectionContext):
      gContext = None
      with testClass.assertRaises(Exception) as context:
        savedVer = storeConnection.saveJSONObject("Test", "123", copy.deepcopy(JSONString), objVerIDToSaveAs)
      testClass.checkGotRightException(context,WrongObjectVersionException)
    storeConnection.executeInsideTransaction(someFn2)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_singleSaveAndRetrieveCommittedTransaction(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='Saved object dosen\'t match')
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectKey, "123", [  ], msg='Returned key wrong')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_singleSaveAndRetrieveUncommittedTransaction(testClass, objectStoreType):
  def someFn(connectionContext):
    savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), None)
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = connectionContext.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='Saved object dosen\'t match')
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectKey, "123", [  ], msg='Returned objectkey wrong')
  savedVer = objectStoreType.executeInsideTransaction(someFn)

def t_saveObjectsInSingleTransaction(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      lastSavedVer = None
      for x in range(1,6):
        savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), lastSavedVer)
        testClass.assertEqual(savedVer, x, msg="Return value of saveJSONObject is not correct")
        lastSavedVer = savedVer
    storeConnection.executeInsideTransaction(someFn)

    #Check object was saved correctly
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='Saved object dosen\'t match')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_creationOfNewObjectNotAffectingOtherObjectTypeWithSameKey(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      savedVer = connectionContext.saveJSONObject("Test1", "123", copy.deepcopy(JSONString), None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test1", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='Saved object dosen\'t match')

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test2", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='creating object id 123 with type Test1 resulted in creation of object with id 123 and type test2')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_saveObjectsInMutipleTransactions(testClass, objectStoreType):
  def dbfn(storeConnection):
    lastSavedVer = None
    for x in range(1,6):
      def someFn(connectionContext):
        savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), lastSavedVer)
        testClass.assertEqual(savedVer, x)
        return savedVer
      lastSavedVer = storeConnection.executeInsideTransaction(someFn)

    #Check object was saved correctly
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, copy.deepcopy(JSONString), [  ], msg='Saved object dosen\'t match')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_creationDateSetCorrectly(testClass, objectStoreType):

  def someFn(connectionContext):
    testDateTime = datetime.datetime.now(pytz.timezone("UTC"))
    testClass.setTestingDateTime(testDateTime)
    savedVer = connectionContext.saveJSONObject("Test", "123", JSONString, None)
    objDict, ver, creationDateTime, lastUpdateDateTime, objectKey = connectionContext.getObjectJSON("Test", "123")
    testClass.assertEqual(objDict, JSONString, msg="Saved object mismatch")
    testClass.assertEqual(ver, savedVer, msg="Saved ver mismatch")
    testClass.assertEqual(creationDateTime, testDateTime, msg="Creation date time wrong")
    testClass.assertEqual(lastUpdateDateTime, testDateTime, msg="Last update date time wrong")
    testClass.assertEqual(objectKey, "123", msg="Object key returned wrong")
  objectStoreType.executeInsideTransaction(someFn)

def t_createExistingObjectFails(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      return connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='object was not added')

    with testClass.assertRaises(Exception) as context:
      savedVer = storeConnection.executeInsideTransaction(someFn)
    testClass.checkGotRightException(context,TryingToCreateExistingObjectException)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_supportsDifferentObjectTypes(testClass, objectStoreType):
  def dbfn(storeConnection):
    ##TODO Confirm this test
    def someFn(connectionContext):
      a = connectionContext.saveJSONObject("TestType1", "123", copy.deepcopy(JSONString), None)
      b = connectionContext.saveJSONObject("TestType2", "123", copy.deepcopy(JSONString2), None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("TestType1", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='object was not added')

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("TestType2", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString2, objectDICT, [  ], msg='object was not added')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_secondSaveObjectSendStringObjectVer(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      return storeConnection.saveJSONObject("Test", "123", JSONString, None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    def someFn2(connectionContext):
      savedVer2 = storeConnection.saveJSONObject("Test", "123", copy.deepcopy(JSONString), str(savedVer))
    storeConnection.executeInsideTransaction(someFn2)
  objectStoreType.executeInsideConnectionContext(dbfn)

#*************************************
#   UpdateJSONObject Tests
#*************************************

def t_updateToDifferentJSONWorks(testClass, objectStoreType):
  def dbfn(storeConnection):
    lastSavedVer = None
    def someFn(connectionContext):
      savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), lastSavedVer)
      return savedVer
    lastSavedVer = storeConnection.executeInsideTransaction(someFn)
    def someFn2(connectionContext):
      savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString2), lastSavedVer)
      return savedVer
    lastSavedVer = storeConnection.executeInsideTransaction(someFn2)

    #Check object was saved correctly
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString2, [  ], msg='Saved object dosen\'t match')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_updateDateSetCorrectly(testClass, objectStoreType):
  def someFn(connectionContext):
    testDateTime = datetime.datetime.now(pytz.timezone("UTC"))
    testClass.setTestingDateTime(testDateTime)
    lastVersion = None
    incTime = testDateTime
    for x in range(1,6):
      testClass.setTestingDateTime(incTime)
      savedVer = connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), lastVersion)
      objDict, ver, creationDateTime, lastUpdateDateTime, objectKey = connectionContext._getObjectJSON("Test", "123")
      testClass.assertEqual(objDict, JSONString)
      testClass.assertEqual(ver, savedVer)
      testClass.assertEqual(creationDateTime, testDateTime, msg="creation time not right")
      testClass.assertEqual(lastUpdateDateTime, incTime, msg="Update time not right")
      testClass.assertEqual(objectKey, "123", msg="object key wrong")
      lastVersion = ver
      incTime = incTime + datetime.timedelta(seconds=60)
  objectStoreType.executeInsideTransaction(someFn)

def t_differentKeys(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      objKeyMap = {}
      for x in range(1,6):
        objKeyMap["1_123" + str(x)] = connectionContext.saveJSONObject("Test", "1_123" + str(x), JSONString, None)
      return objKeyMap
    objKeyMap = storeConnection.executeInsideTransaction(someFn)

    #update 3rd object to alternative data
    def someFn(connectionContext):
      connectionContext.saveJSONObject("Test", "1_123" + str(3), copy.deepcopy(JSONString2), objKeyMap["1_123" + str(3)])
    storeConnection.executeInsideTransaction(someFn)

    for x in range(1,6):
      expRes = copy.deepcopy(JSONString)
      if x==3:
        expRes = copy.deepcopy(JSONString2)
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "1_123" + str(x))
      testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, expRes, [  ], msg='Saved object dosen\'t match')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_updateUsingFunctionOutsideOfTransactionFails(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      return connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    def updateFn(obj, connectionContext):
      testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, obj, [  ], msg='Saved object dosen\'t match')
      return JSONString2

    with testClass.assertRaises(Exception) as context:
      newVer = storeConnection.updateJSONObject("Test", "123", updateFn, savedVer)
    testClass.checkGotRightException(context,UnallowedMutationException)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_updateUsingFunction(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      return connectionContext.saveJSONObject("Test", "123", copy.deepcopy(JSONString), None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    def updateFn(obj, connectionContext):
      testClass.assertJSONStringsEqualWithIgnoredKeys(copy.deepcopy(JSONString), obj, [  ], msg='Saved object dosen\'t match')
      return JSONString2

    def someFn(connectionContext):
      newVer = connectionContext.updateJSONObject("Test", "123", updateFn, savedVer)
    storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(copy.deepcopy(JSONString2), objectDICT, [  ], msg='object was not updated')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_updateUpdatesCorrectObjecTypet(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      savedVer1 = connectionContext.saveJSONObject("TestType1", "123", JSONString, None)
      savedVer2 = connectionContext.saveJSONObject("TestType2", "123", JSONString, None)
      return savedVer1, savedVer2
    savedVer1, savedVer2 = storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("TestType1", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='object1 was not added')
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("TestType2", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='object2 was not added')

    #Update TestType1
    def updateFn(obj, connectionContext):
      return JSONString2

    def someFn(connectionContext):
      newVer = connectionContext.updateJSONObject("TestType1", "123", updateFn, savedVer1)
    storeConnection.executeInsideTransaction(someFn)

    #Verify Type1 was changed and Type2 remains the same
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("TestType1", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString2, objectDICT, [  ], msg='object was not updated')
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("TestType2", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='object with different key was updated')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_updateMissingVersionAssumedSafe(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      return storeConnection.saveJSONObject("Test", "123", JSONString, None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    def updateFn(obj, connectionContext):
      return JSONString2
    def someFn(connectionContext):
      newVer = connectionContext.updateJSONObject("Test", "123", updateFn, None)
    storeConnection.executeInsideTransaction(someFn)
  objectStoreType.executeInsideConnectionContext(dbfn)


#*************************************
#   RemoveJSONObject Tests
#*************************************


def t_removeMissingObject(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      newVer = connectionContext.removeJSONObject("Test", "123", 1, False)
    with testClass.assertRaises(Exception) as context:
      storeConnection.executeInsideTransaction(someFn)
    testClass.checkGotRightException(context,TriedToDeleteMissingObjectException)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_removeMissingObjectIgnoreMissing(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      newVer = connectionContext.removeJSONObject("Test", "123", 1, True)
    storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='object was not removed')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_removeObjectWithNoTransactionFails(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      return connectionContext.saveJSONObject("Test", "123", JSONString, None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='object was not added')

    with testClass.assertRaises(Exception) as context:
      newVer = storeConnection.removeJSONObject("Test", "123", savedVer, False)
    testClass.checkGotRightException(context,UnallowedMutationException)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_removeObject(testClass, objectStoreType):
  def dbfn(storeConnection):
    def someFn(connectionContext):
      return connectionContext.saveJSONObject("Test", "123", JSONString, None)
    savedVer = storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(JSONString, objectDICT, [  ], msg='object was not added')

    def someFn(connectionContext):
      newVer = connectionContext.removeJSONObject("Test", "123", savedVer, False)
    storeConnection.executeInsideTransaction(someFn)

    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='object was not removed')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_removeObjectOnlyRemovesKeyOfSameObjectType(testClass, objectStoreType):
  def dbfn(storeConnection):
    #create 2 objects
    def someFn(connectionContext):
      savedVer1 = connectionContext.saveJSONObject("Test1", "123", JSONString, None)
      savedVer2 = connectionContext.saveJSONObject("Test2", "123", JSONString2, None)
      return (savedVer1, savedVer2)
    (savedVer1, savedVer2) = storeConnection.executeInsideTransaction(someFn)

    #check two different created objects exist (same key)
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test1", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='object 1 not ok')
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test2", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString2, [  ], msg='object 2 not ok')

    #Remove Object 1
    def someFn(connectionContext):
      newVer = connectionContext.removeJSONObject("Test1", "123", savedVer1, False)
    storeConnection.executeInsideTransaction(someFn)

    #Make sure object 1 is not there and object 2 is there
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test1", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='object was not removed')
    (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = storeConnection.getObjectJSON("Test2", "123")
    testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString2, [  ], msg='object2 should not have been removed')
  objectStoreType.executeInsideConnectionContext(dbfn)

#*************************************
#   getPaginatedResult Tests
#*************************************

def addSampleRow(storeConnection, num, bbStringFN, objectType="Test1"):
  def someFn(connectionContext):
    toInsert = copy.deepcopy(JSONString)
    toInsert['AA'] = num
    toInsert['BB'] = bbStringFN(num)
    xres = connectionContext.saveJSONObject(objectType, "123" + str(num), toInsert, None)
  storeConnection.executeInsideTransaction(someFn)


def addSampleRows2(storeConnection, numRows, bbStringFN, offset=0, objectType="Test1"):
  def someFn(connectionContext):
    for x in range(offset,numRows + offset):
      toInsert = copy.deepcopy(JSONString)
      toInsert['AA'] = x
      toInsert['BB'] = bbStringFN(x)
      xres = connectionContext.saveJSONObject(objectType, "123" + str(x), toInsert, None)
  storeConnection.executeInsideTransaction(someFn)

def addSampleRows(storeConnection, numRows, bbString='BB', offset=0, objectType="Test1"):
  def bbStringFN(x):
    return bbString
  addSampleRows2(storeConnection, numRows, bbStringFN, offset, objectType)

def assertCorrectPaginationResult(testClass, result, expectedOffset, expectedPageSize, expectedTotal):
  testClass.assertEqual(result['pagination']['offset'], expectedOffset, msg='Wrong offset in pagination')
  testClass.assertEqual(result['pagination']['pagesize'], expectedPageSize, msg='Wrong pagesize in pagination')
  if result['pagination']['total'] != expectedTotal:
    print("           Total was:", result['pagination']['total'])
    print("          Offset was:", result['pagination']['offset'])
    print("       Page Size was:", result['pagination']['pagesize'])
    print("  num Actual results:", len(result['result']))
    print("    expectedPageSize:", expectedPageSize)
    testClass.assertEqual(result['pagination']['total'], (expectedOffset + expectedPageSize + 1), msg='Total is wrong, should be actual total (' + str(expectedTotal) + ') or one more than can be displayed in this page')

def t_getPaginatedResultsNoData(testClass, objectStoreType):
  def dbfn(storeConnection):
    def outputFN(item):
      return item
    paginatedParamValues = {
      'offset': 0,
      'pagesize': 10,
      'query': '',
      'sort': ''
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = {'pagination': {'offset': 0, 'pagesize': 10, 'total': 0}, 'result': []}
    testClass.assertJSONStringsEqualWithIgnoredKeys(res, expectedRes, [  ], msg='Wrong result')
  objectStoreType.executeInsideConnectionContext(dbfn)

#Test with five rows all in one
def t_getPaginatedResultsFiveRowsInOneHit(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 0,
      'pagesize': 10,
      'query': '',
      'sort': None
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    for x in range(0,5):
      expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})
    assertCorrectPaginationResult(testClass, res, 0, 10, 5)

    a = list(map(lambda x: json.dumps(x), res['result']))
    b = list(map(lambda x: json.dumps(x), expectedRes))
    testClass.assertTrue(objectsEqual(a, b), msg="Wrong result")

  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getPaginatedResultsFiveRowsInOneHitWithPagesize(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 0,
      'pagesize': 5,
      'query': '',
      'sort': 'AA'
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    for x in range(0,5):
      expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})
    assertCorrectPaginationResult(testClass, res, 0, 5, 5)

    a = list(map(lambda x: json.dumps(x), res['result']))
    b = list(map(lambda x: json.dumps(x), expectedRes))
    testClass.assertTrue(objectsEqual(a, b), msg="Wrong result")

  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getPaginatedResultsFiveRowsPagesize2offset0(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 0,
      'pagesize': 2,
      'query': '',
      'sort': 'AA'
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    for x in range(0,2):
      expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})
    assertCorrectPaginationResult(testClass, res, 0, 2, 5)

    a = list(map(lambda x: json.dumps(x), res['result']))
    b = list(map(lambda x: json.dumps(x), expectedRes))
    print("  actual:", a)
    print("expected:", b)
    testClass.assertTrue(objectsEqual(a, b), msg="Wrong result")

  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getPaginatedResultsFiveRowsPagesize2offset2(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 2,
      'pagesize': 2,
      'query': '',
      'sort': 'AA'
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    for x in range(2,4):
      expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})
    assertCorrectPaginationResult(testClass, res, 2, 2, 3) # Changed from 5 as when we use iterator total is not correct
    testClass.assertJSONStringsEqualWithIgnoredKeys(res['result'], expectedRes, [  ], msg='Wrong result')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getPaginatedResultsFiveRowsPagesize2offset4(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 4,
      'pagesize': 2,
      'query': '',
      'sort': 'AA'
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    for x in range(4,5):
      expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})
    assertCorrectPaginationResult(testClass, res, 4, 2, 5)
    testClass.assertJSONStringsEqualWithIgnoredKeys(res['result'], expectedRes, [  ], msg='Wrong result')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getPaginatedResultsFiveRowsPagesize2offset10(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 10,
      'pagesize': 2,
      'query': '',
      'sort': 'AA'
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    assertCorrectPaginationResult(testClass, res, 10, 2, 5)
    testClass.assertJSONStringsEqualWithIgnoredKeys(res['result'], expectedRes, [  ], msg='Wrong result')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_UpdateFilter(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5, 'yyYYyyy')
    addSampleRows(storeConnection, 5, 'xxxxxxx', 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 0,
      'pagesize': 10,
      'query': 'yyyyyyy',
      'sort': None
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    for x in range(0,5):
      expectedRes.append({"AA": x, "BB": "yyYYyyy", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})
    assertCorrectPaginationResult(testClass, res, 0, 10, 5)
    #order dosen't matter in this result
    for curRes in res['result']:
      found = False
      for x in range(0, len(expectedRes)):
        if objectsEqual(expectedRes[x],curRes):
          found = True
          del expectedRes[x]
          break
      print(found, curRes)
    testClass.assertEqual(0, len(expectedRes), msg="Not all expected results were in the response")
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_filterAllResultsOut(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5, 'yyYYyyy')
    addSampleRows(storeConnection, 5, 'xxxxxxx', 5)
    def outputFN(item):
      return item[0]
    paginatedParamValues = {
      'offset': 0,
      'pagesize': 10,
      'query': 'dfgdbdfgfgfvfdgfd',
      'sort': None
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
    expectedRes = []
    assertCorrectPaginationResult(testClass, res, 0, 10, 0)
    testClass.assertJSONStringsEqualWithIgnoredKeys(res['result'], expectedRes, [  ], msg='Wrong result')
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getAllRowsForObjectType(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    filterFN = None

    resALL = storeConnection.getAllRowsForObjectType("Test1", filterFN, outputFN, None)

    expectedRes = []
    for x in range(0,5):
      expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})

    actualResSorted = [{},{},{},{},{}]
    for x in range(0,5):
      actualResSorted[resALL[x]["AA"]] = resALL[x]

    testClass.assertEqual(actualResSorted, expectedRes)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getAllRowsForObjectType(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    filterFN = None

    resALL = storeConnection.getAllRowsForObjectType("Test1", filterFN, outputFN, None)
    expectedRes = []
    for x in range(0,5):
      expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})

    actualResSorted = [{},{},{},{},{}]
    for x in range(0,len(resALL)):
      actualResSorted[resALL[x]["AA"]] = resALL[x]

    testClass.assertEqual(actualResSorted, expectedRes)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getAllRowsForObjectType_FilterByFN(testClass, objectStoreType):
  def dbfn(storeConnection):
    addSampleRows(storeConnection, 5)
    def outputFN(item):
      return item[0]
    def filterFN(item, whereClauseText):
      if item[0]["AA"] == 3:
        return False
      return True

    resALL = storeConnection.getAllRowsForObjectType("Test1", filterFN, outputFN, None)
    expectedRes = []
    for x in range(0,5):
      if x == 3:
        expectedRes.append({})
      else:
        expectedRes.append({"AA": x, "BB": "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})

    actualResSorted = [{},{},{},{},{}]
    for x in range(0,len(resALL)):
      actualResSorted[resALL[x]["AA"]] = resALL[x]

    testClass.assertEqual(actualResSorted, expectedRes)
  objectStoreType.executeInsideConnectionContext(dbfn)

def t_getAllRowsForObjectType_FilterByWhereClause(testClass, objectStoreType):
  def dbfn(storeConnection):
    def bbStringFN(x):
      return "BB" + str(x) + "BB"
    addSampleRows2(storeConnection, 5, bbStringFN)
    def outputFN(item):
      return item[0]

    resALL = storeConnection.getAllRowsForObjectType("Test1", None, outputFN, "BB3BB")
    expectedRes = []
    for x in range(0,5):
      if x != 3:
        expectedRes.append({})
      else:
        expectedRes.append({"AA": x, "BB": "BB" + str(x) + "BB", "CC": {"CC.AA": "AA", "CC.BB": "BB", "CC.CC": "CC"}})

    actualResSorted = [{},{},{},{},{}]
    for x in range(0,len(resALL)):
      actualResSorted[resALL[x]["AA"]] = resALL[x]

    testClass.assertEqual(actualResSorted, expectedRes)
  objectStoreType.executeInsideConnectionContext(dbfn)


def t_listAllObjectTypes_ZeroTypes(testClass, objectStoreType):
  def dbfn(storeConnection):
    objTypes = storeConnection.list_all_objectTypes()
    testClass.assertEqual(len(objTypes),0,'Returned objectTypes when there should not be any')

  objectStoreType.executeInsideTransaction(dbfn)


def t_listAllObjectTypes_MutipleTypes(testClass, objectStoreType):
  def dbfn(storeConnection):
    xres = storeConnection.saveJSONObject("Test1", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test1", "124", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test2", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test3", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test4", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test5", "123", {"S":12}, None)

    objTypes = storeConnection.list_all_objectTypes()
    expectedLis = ["Test1", "Test2", "Test3", "Test4", "Test5"]
    if not objectsEqual(objTypes, expectedLis):
      print("Got list:", objTypes)
      print("Expected list:", expectedLis)
      testClass.assertTrue(objectsEqual(objTypes, expectedLis), msg="Wrong result")

  objectStoreType.executeInsideTransaction(dbfn)

def tt_listAllObjectTypes_MutipleTypesSaveAndLoad(testClass, getObjFn, ConfigDict):
  def dbfn(storeConnection):
    xres = storeConnection.saveJSONObject("Test1", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test1", "124", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test2", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test3", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test4", "123", {"S":12}, None)
    xres = storeConnection.saveJSONObject("Test5", "123", {"S":12}, None)

    objTypes = storeConnection.list_all_objectTypes()
    expectedLis = ["Test1", "Test2", "Test3", "Test4", "Test5"]
    if not objectsEqual(objTypes, expectedLis):
      print("Got list:", objTypes)
      print("Expected list:", expectedLis)
      testClass.assertTrue(objectsEqual(objTypes, expectedLis), msg="Wrong result")

  obj1 = getObjFn(ConfigDict, resetData=True)
  obj1.executeInsideTransaction(dbfn)
  obj1 = None


  def dbfn(storeConnection):
    objTypes = storeConnection.list_all_objectTypes()
    expectedLis = ["Test1", "Test2", "Test3", "Test4", "Test5"]
    if not objectsEqual(objTypes, expectedLis):
      print("Got list:", objTypes)
      print("Expected list:", expectedLis)
      testClass.assertTrue(objectsEqual(objTypes, expectedLis), msg="Object list not retrived on DB reload")

  obj2 = getObjFn(ConfigDict, resetData=False)
  obj2.executeInsideTransaction(dbfn)

def t_fullAsscendingSortWorks(testClass, objectStoreType):
  def bbStringFN(x):
    return str(x)

  def outputFN(item):
    return item[0]

  def dbfn(storeConnection):
    # Adding row in no order
    addSampleRow(storeConnection, 1, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 8, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 3, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 4, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 5, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 6, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 2, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 7, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 9, bbStringFN, objectType="Test1")

    paginatedParamValues = {
      'offset': 0,
      'pagesize': 20,
      'query': '',
      'sort': 'AA'
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)

    expectedOrder = [1,2,3,4,5,6,7,8,9]
    idx = 0
    for x in res["result"]:
      testClass.assertEqual(x["AA"], expectedOrder[idx], msg="Returned idx " + str(idx) + " wrong")
      idx = idx + 1


  objectStoreType.executeInsideConnectionContext(dbfn)

def t_fullDescendingSortWorks(testClass, objectStoreType):
  def bbStringFN(x):
    return str(x)

  def outputFN(item):
    return item[0]

  def dbfn(storeConnection):
    # Adding row in no order
    addSampleRow(storeConnection, 1, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 8, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 3, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 4, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 5, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 6, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 2, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 7, bbStringFN, objectType="Test1")
    addSampleRow(storeConnection, 9, bbStringFN, objectType="Test1")

    paginatedParamValues = {
      'offset': 0,
      'pagesize': 20,
      'query': '',
      'sort': 'AA:desc'
    }
    res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)

    expectedOrder = [9,8,7,6,5,4,3,2,1]
    idx = 0
    for x in res["result"]:
      testClass.assertEqual(x["AA"], expectedOrder[idx], msg="Returned idx " + str(idx) + " wrong")
      idx = idx + 1


  objectStoreType.executeInsideConnectionContext(dbfn)

  def t_partDescendingSortWorks(testClass, objectStoreType):
    def bbStringFN(x):
      return str(x)

    def outputFN(item):
      return item[0]

    def dbfn(storeConnection):
      # Adding row in no order
      addSampleRow(storeConnection, 1, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 8, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 3, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 4, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 5, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 6, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 2, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 7, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 9, bbStringFN, objectType="Test1")

      paginatedParamValues = {
        'offset': 5,
        'pagesize': 20,
        'query': '',
        'sort': 'AA:desc'
      }
      res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)

      expectedOrder = [4,3,2,1]
      idx = 0
      for x in res["result"]:
        testClass.assertEqual(x["AA"], expectedOrder[idx], msg="Returned idx " + str(idx) + " wrong")
        idx = idx + 1


    objectStoreType.executeInsideConnectionContext(dbfn)

  def t_startDescendingSortWorks(testClass, objectStoreType):
    def bbStringFN(x):
      return str(x)

    def outputFN(item):
      return item[0]

    def dbfn(storeConnection):
      # Adding row in no order
      addSampleRow(storeConnection, 1, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 8, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 3, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 4, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 5, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 6, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 2, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 7, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 9, bbStringFN, objectType="Test1")

      paginatedParamValues = {
        'offset': 0,
        'pagesize': 3,
        'query': '',
        'sort': 'AA:desc'
      }
      res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)

      expectedOrder = [9,8,7]
      idx = 0
      for x in res["result"]:
        testClass.assertEqual(x["AA"], expectedOrder[idx], msg="Returned idx " + str(idx) + " wrong")
        idx = idx + 1


    objectStoreType.executeInsideConnectionContext(dbfn)

  def t_midDescendingSortWorks(testClass, objectStoreType):
    def bbStringFN(x):
      return str(x)

    def outputFN(item):
      return item[0]

    def dbfn(storeConnection):
      # Adding row in no order
      addSampleRow(storeConnection, 1, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 8, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 3, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 4, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 5, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 6, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 2, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 7, bbStringFN, objectType="Test1")
      addSampleRow(storeConnection, 9, bbStringFN, objectType="Test1")

      paginatedParamValues = {
        'offset': 4,
        'pagesize': 3,
        'query': '',
        'sort': 'AA:desc'
      }
      res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)

      expectedOrder = [6,5,4]
      idx = 0
      for x in res["result"]:
        testClass.assertEqual(x["AA"], expectedOrder[idx], msg="Returned idx " + str(idx) + " wrong")
        idx = idx + 1


    objectStoreType.executeInsideConnectionContext(dbfn)
