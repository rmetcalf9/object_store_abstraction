import TestHelperSuperClass
import test_objectStores_GenericTests as genericTests
import object_store_abstraction as undertest

ConfigDict = {
  "Type": "Caching",
  ##"Caching": {**}, #If missing memory is used
  "Main": {
    "Type": "Memory" #For testing main store is memory
     #in real uses it would not be
  }, #Main persistant store
  "DefaultPolicy": {
    "cache": True,
    "maxCacheSize": 100,
    "cullToSize": 50,
    "timeout": 1000 #in miliseconds 1000 = 1 second
  },
  "ObjectTypeOverride": {
    "bad": {
      "cache": True,
      "maxCacheSize": 100,
      "cullToSize": 50,
      "timeout": 1000 #in miliseconds 1000 = 1 second
    }
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
