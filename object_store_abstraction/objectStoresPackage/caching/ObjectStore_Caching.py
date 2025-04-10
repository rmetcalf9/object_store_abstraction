from object_store_abstraction import ObjectStore, ObjectStoreConfigError
from .ConnectionContext import ConnectionContext
import copy
from .CachePolicy import CachePolicyClass
from .CullQueues import CullQueuesClass

class ObjectStore_Caching(ObjectStore):
  defaultPolicy = None
  overridePolicies = None
  cachingStore = None
  mainStore = None
  cullQueues = None

  #For doubleStringIndex use same logic as memory store for the cache
  # the cache won't expire
  doubleStringIndexCache = None
  #structure:
  # {
  #  objTypeSyt: {
  #     byA: {},
  #     byB: {}
  #  }
  # }
  def _INT_getDictForDoubleStringIndexCache(self):
    return self.doubleStringIndexCache


  def __init__(self, configJSON, externalFns, detailLogging, type, factoryFn):
    super(ObjectStore_Caching, self).__init__(externalFns, detailLogging, type)
    self.doubleStringIndexCache = {}

    self.cullQueues = CullQueuesClass()

    requiredConfigParams = ["Main", "DefaultPolicy", "ObjectTypeOverride"]
    for x in requiredConfigParams:
      if x not in configJSON:
        raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + x + " missing")

    self.defaultPolicy = CachePolicyClass(copy.deepcopy(configJSON["DefaultPolicy"]), errorName="DefaultPolicy")
    self.overridePolicies = {}
    for objectType in configJSON["ObjectTypeOverride"]:
      self.overridePolicies[objectType] = CachePolicyClass(copy.deepcopy(configJSON["ObjectTypeOverride"][objectType]), errorName="objectTypeOverride " + objectType + " ObjectTypeOverride")

    cachingConfigDict = { "Type": "Memory" }
    if "Caching" in configJSON:
      cachingConfigDict = configJSON["Caching"]

    self.cachingStore = factoryFn(
      cachingConfigDict,
      externalFns,
      detailLogging=detailLogging
    )
    self.mainStore = factoryFn(
      configJSON["Main"],
      externalFns,
      detailLogging=detailLogging
    )

    # ##"Caching": {**}, #If missing memory is used
    # "Main": {}, #Main persistant store
    # "DefaultPolicy": {
    #   "cache": True,
    #   "maxCacheSize": 100,
    #   "cullToSize": 50
    # },
    # "ObjectTypeOverride": {
    # }


  def getPolicy(self, objectType):
    if objectType in self.overridePolicies:
      return self.overridePolicies[objectType]
    return self.defaultPolicy


  def _getConnectionContext(self):
    return ConnectionContext(self)