from TestHelperSuperClass import testHelperSuperClass

import object_store_abstraction as undertest
import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests

ConfigDict = {
  "Type": "SimpleFileStore",
  "BaseLocation": "./tests/SimpleFileStore"
}


class objectStoresSimpleFileStore(testHelperSuperClass):
  def test_genericSimpleFileStoreTests(self):
    def getObjFn(ConfigDict):
      #print("AAA", ConfigDict)
      #self.AssertTrue(False);
      obj = undertest.ObjectStore_SimpleFileStore(ConfigDict, self.getObjectStoreExternalFns())
      obj.resetDataForTest()
      # print("Test_SFS 23: Resetting")
      return obj
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict)

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
