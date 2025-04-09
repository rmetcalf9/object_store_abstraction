import TestHelperSuperClass

import object_store_abstraction as undertest
import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests
import test_objectStores_GenericTests_DoubleStringIndex as genericTestsDoubleStringIndex

ConfigDict = {
  "Type": "SimpleFileStore",
  "BaseLocation": "./tests/SimpleFileStore"
}

class objectStoresSimpleFileStore(TestHelperSuperClass.testHelperSuperClass):
  def test_genericSimpleFileStoreTests(self):
    def getObjFn(ConfigDict, resetData = True):
      #print("AAA", ConfigDict)
      #self.AssertTrue(False);
      obj = undertest.ObjectStore_SimpleFileStore(ConfigDict, self.getObjectStoreExternalFns(), False, type='testSFS', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      # print("Test_SFS 23: Resetting")
      return obj
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict)

  def test_genericTests_doublestringindex(self):
    def getObjFn(ConfigDict, resetData = True):
      obj = undertest.ObjectStore_SimpleFileStore(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSFS', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      return obj
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, ConfigDict)


  def test_creationWithMissingBaseLocationFails(self):
    ConfigDictI = {
      "Type": "SimpleFileStore"
    }
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation param Missing", msg="Wrong error message")


  def test_creationWithInvalidBaseLocationFails(self):
    ConfigDictI = {
      "Type": "SimpleFileStore",
      "BaseLocation": "saddfg.g/fdagtgastore"
    }
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation not found", msg="Wrong error message")

  def test_creationWithInvalidBaseLocationFails(self):
    ConfigDictI = {
      "Type": "SimpleFileStore",
      "BaseLocation": "./tests/TestHelperSuperClass.py"
    }
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation is not a directory", msg="Wrong error message")

  def test_simpleCreationBasePAthWithTrailingSlash(self):
    ConfigDictI = copy.deepcopy(ConfigDict)
    ConfigDictI["BaseLocation"] = ConfigDictI["BaseLocation"] + "/"
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation should not end with slash", msg="Wrong error message")

  def test_simpleCreationBasePAthWithTrailingBackSlash(self):
    ConfigDictI = copy.deepcopy(ConfigDict)
    ConfigDictI["BaseLocation"] = ConfigDictI["BaseLocation"] + "\\"
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation should not end with backslash", msg="Wrong error message")

  def test_simpleCreationWithNoOperation(self):
    undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())


  def test_dockJobBug(self):
    objectType = "jobsData"

    storeConnection = undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())

    class jobClass() :
      objectVersion = None
      def _caculatedDict(self, appObj):
        return {
          "aa": "fff",
          "bb": "fff22",
        }


    jobGUID = 'abc123'
    jobs = {}
    jobs[jobGUID] = jobClass()

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      newObjectVersion = connectionContext.saveJSONObject(
        objectType,
        jobGUID,
        jobs[jobGUID]._caculatedDict(appObj=None),
        objectVersion = jobs[jobGUID].objectVersion
      )
      jobs[jobGUID].objectVersion = newObjectVersion
    storeConnection.executeInsideTransaction(someFn)


    def someFn(connectionContext):
      paginatedParamValues = {
        'offset': 0,
        'pagesize': 100000,
        'query': None,
        'sort': None,
      }
      loadedData = connectionContext.getPaginatedResult(objectType, paginatedParamValues=paginatedParamValues, outputFN=None)
      ##print(loadedData)
      print("Found " + str(len(loadedData["result"])) + " jobs in datastore")

    storeConnection.executeInsideTransaction(someFn)
    ##self.assertTrue(False)

  def test_ObjectRemovalInNewInstance(self):
    objectType = "chartnames"
    keyToTest = "usr::linkvisAutoconfigTestUser/:_/untitle  dsdsWITHPACES"
    someDict = { "d": "d"}

    storeConnection = undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())
    storeConnection.resetDataForTest()
    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      newObjectVersion = connectionContext.saveJSONObject(
        objectType,
        keyToTest,
        someDict,
        objectVersion = None
      )
      self.assertEqual(newObjectVersion, 1)
    storeConnection.executeInsideTransaction(someFn)

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      obj, objVersion, creationDateTime, lastUpdateDateTime, objKey = connectionContext.getObjectJSON(objectType,keyToTest)
      self.assertEqual(obj, someDict)
      return objVersion
    objVersion = storeConnection.executeInsideTransaction(someFn)

    storeConnection2 = undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      newObjectVersion = connectionContext.removeJSONObject(
        objectType=objectType,
        objectKey=keyToTest,
        objectVersion=objVersion,
        ignoreMissingObject=False
      )
      self.assertEqual(newObjectVersion, None)
    storeConnection2.executeInsideTransaction(someFn)

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      obj, objVersion, creationDateTime, lastUpdateDateTime, objKey = connectionContext.getObjectJSON(objectType,keyToTest)
      self.assertEqual(obj, None)
    storeConnection2.executeInsideTransaction(someFn)

  def test_remove_objectversionmismatch(self):
    objectType = "chartnames"
    keyToTest = "usr::linkvisAutoconfigTestUser/:_/untitle  dsdsWITHPACES"
    someDict = { "d": "d"}

    storeConnection = undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())
    storeConnection.resetDataForTest()
    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      return connectionContext.saveJSONObject(
        objectType,
        keyToTest,
        someDict,
        objectVersion = None
      )
    newObjectVersion = storeConnection.executeInsideTransaction(someFn)

    def removeButWithWrongObjectVersion(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      return connectionContext.removeJSONObject(
        objectType=objectType,
        objectKey=keyToTest,
        objectVersion=123,
        ignoreMissingObject=False
      )

    with self.assertRaises(Exception) as context:
      newObjectVersion = storeConnection.executeInsideTransaction(removeButWithWrongObjectVersion)
    self.checkGotRightExceptionType(context,undertest.WrongObjectVersionException)
