from object_store_abstraction import ObjectStoreConfigError
import time

class CachePolicyClass():
  caching = None
  def __init__(self, policy, errorName):
    if "cache" not in policy:
      raise ObjectStoreConfigError(
        "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " cache missing")
    if not isinstance(policy["cache"], bool):
      raise ObjectStoreConfigError(
        "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " cache must be True or False")

    if policy["cache"]:
      policyRequiredElements = ["maxCacheSize", "cullToSize", "timeout"]
      for x in policyRequiredElements:
        if x not in policy:
          raise ObjectStoreConfigError(
            "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " " + x + " missing")

      if not isinstance(policy["maxCacheSize"], int):
        raise ObjectStoreConfigError(
          "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " maxCacheSize must be int")
      if not isinstance(policy["cullToSize"], int):
        raise ObjectStoreConfigError(
          "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " cullToSize must be int")
      if policy["cullToSize"] >= policy["maxCacheSize"]:
        raise ObjectStoreConfigError(
          "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " maxCacheSize must be greater than cullToSize")
      if not isinstance(policy["timeout"], int):
        raise ObjectStoreConfigError(
          "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " timeout must be int")
      if policy["timeout"]<=0:
        raise ObjectStoreConfigError(
          "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " timeout must be greater than 0")
    self.caching = policy["cache"]

  def __isCaching(self):
    return self.caching

  def __putObjectIntoCache(self, objectType, objectKey, JSONString, objectVersion, cacheContext):
    dictToStore = {
      "exp": time.perf_counter(),
      "d": JSONString
    }
    cacheContext._saveJSONObject(objectType, objectKey, dictToStore, objectVersion)

  def __preformCull(self, cacheContext):
    raise Exception("Not Implemented __preformCull")

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject, cacheContext):
    if not self.__isCaching():
      return None
    cacheContext._removeJSONObject(objectType, objectKey, objectVersion, ignoreMissingObject=True)
    return None # return value not used

  def _saveJSONObject(self, objectType, objectKey, JSONString, objectVersion, cacheContext):
    if not self.__isCaching():
      return None
    self.__putObjectIntoCache(objectType, objectKey, JSONString, objectVersion, cacheContext)
    self.__preformCull(cacheContext)
    return None

  def _getObjectJSON(self, objectType, objectKey, cacheContext, mainContext):
    if not self.__isCaching():
      return mainContext._getObjectJSON(objectType, objectKey)

    # get From Cache

    # If it has not expired then return cache value

    # If it is expired lookup from main context

    raise Exception("Not Implemented _getObjectJSON")
    ##self.__putObjectIntoCache(objectType, objectKey, JSONString, objectVersion, cacheContext)
    self.__preformCull(cacheContext)

    # Return main context value


