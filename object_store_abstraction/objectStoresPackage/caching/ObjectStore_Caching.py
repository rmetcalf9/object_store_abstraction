from object_store_abstraction import ObjectStore, ObjectStoreConfigError
from .ConnectionContext import ConnectionContext
import copy

class ObjectStore_Caching(ObjectStore):
  defaultPolicy = None
  overridePolicies = None

  def __init__(self, configJSON, externalFns, detailLogging, type, factoryFn):
    super(ObjectStore_Caching, self).__init__(externalFns, detailLogging, type)

    requiredConfigParams = ["Main", "DefaultPolicy", "ObjectTypeOverride"]
    for x in requiredConfigParams:
      if x not in configJSON:
        raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + x + " missing")
    def checkPolicyElementsOK(policy, name):
      policyRequiredElements = ["cache", "maxCacheSize", "cullToSize"]
      for x in policyRequiredElements:
        if x not in policy:
          raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + name + " " + x + " missing")
        if not isinstance(policy["cache"], bool):
          raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + name + " cache must be True or False")
        if policy["cache"]:
          if not isinstance(policy["maxCacheSize"], int):
            raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + name + " maxCacheSize must be int")
          if not isinstance(policy["cullToSize"], int):
            raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + name + " cullToSize must be int")
          if policy["cullToSize"] >= policy["maxCacheSize"]:
            raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + name + " maxCacheSize must be greater than cullToSize")

    checkPolicyElementsOK(policy=configJSON["DefaultPolicy"], name="DefaultPolicy")
    for tableName in configJSON["ObjectTypeOverride"]:
      checkPolicyElementsOK(policy=configJSON["ObjectTypeOverride"][tableName], name=tableName + " policy")

    self.defaultPolicy = copy.deepcopy(configJSON["DefaultPolicy"])
    self.overridePolicies = copy.deepcopy(configJSON["ObjectTypeOverride"])

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