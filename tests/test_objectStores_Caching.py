import TestHelperSuperClass
import test_objectStores_GenericTests as genericTests
import object_store_abstraction as undertest

ConfigDict = {
  "Type": "Caching",
  ##"Caching": {**}, #If missing memory is used
  "Main": {}, #Main persistant store
  "DefaultPolicy": {
    "cache": True,
    "maxCacheSize": 100,
    "cullToSize": 50
  },
  "TablePolicyOverride": {
  }
}

class helper(TestHelperSuperClass.testHelperSuperClass):
  pass

@TestHelperSuperClass.wipd
class test_objectStoresMigrating(helper):
  def test_genericTests(self):
    def getObjFn(ConfigDict, resetData = True):
      return undertest.ObjectStore_Caching(ConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testMEM', factoryFn=undertest.createObjectStoreInstance)
    genericTests.runAllGenericTests(self, getObjFn, ConfigDict, expectPersistance=False)
