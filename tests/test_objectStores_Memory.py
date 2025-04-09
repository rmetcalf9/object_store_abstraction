import TestHelperSuperClass

import object_store_abstraction as undertest
import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests
import test_objectStores_GenericTests_DoubleStringIndex as genericTestsDoubleStringIndex

ConfigDict = {}

#@TestHelperSuperClass.wipd
class test_objectStoresMemory(TestHelperSuperClass.testHelperSuperClass):
  def test_genericTests(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Memory(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict, expectPersistance=False)

  def test_genericTests_doublestringindex(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Memory(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, ConfigDict)
