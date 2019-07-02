from TestHelperSuperClass import testHelperSuperClass

import object_store_abstraction as undertest
import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests

ConfigDict = {}

class test_objectStoresMemory(testHelperSuperClass):
  def test_genericTests(self):
    def getObjFn(ConfigDict):
      return undertest.ObjectStore_Memory(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict)
