from TestHelperSuperClass import testHelperSuperClass

import object_store_abstraction as undertest
import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests

ConfigDict = {
  "Type": "SimpleFileStore",
  "BaseLocation": "./datastore"
}


class objectStoresSimpleFileStore(testHelperSuperClass):
  '''
  def test_genericSimpleFileStoreTests(self):
    def getObjFn(ConfigDict):
      #print("AAA", ConfigDict)
      #self.AssertTrue(False);
      return undertest.ObjectStore_SimpleFileStore(ConfigDict, self.getObjectStoreExternalFns())
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict)
  '''

  def test_creationWithMissingBaseLocationFails(self):
    ConfigDict = {
      "Type": "SimpleFileStore"
    }
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation param Missing", msg="Wrong error message")


  def test_creationWithInvalidBaseLocationFails(self):
    ConfigDict = {
      "Type": "SimpleFileStore",
      "BaseLocation": "saddfg.g/fdagtgastore"
    }
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation not found", msg="Wrong error message")

  def test_creationWithInvalidBaseLocationFails(self):
    ConfigDict = {
      "Type": "SimpleFileStore",
      "BaseLocation": "./tests/TestHelperSuperClass.py"
    }
    with self.assertRaises(Exception) as context:
      undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigError)
    self.assertEqual(str(context.exception),"APIAPP_OBJECTSTORECONFIG SimpleFileStore ERROR - BaseLocation is not a directory", msg="Wrong error message")

  def test_simpleCreationWithNoOperation(self):
    ConfigDict = {
      "Type": "SimpleFileStore",
      "BaseLocation": "./tests/SimpleFileStore"
    }
    undertest.createObjectStoreInstance(ConfigDict, self.getObjectStoreExternalFns())
