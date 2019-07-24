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

class test_objectStoresMigrating(testHelperSuperClass):
  @wipd
  def test_genericTests(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Migrating(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMIG', factoryFn=undertest.createObjectStoreInstance)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict, expectPersistance=False)
