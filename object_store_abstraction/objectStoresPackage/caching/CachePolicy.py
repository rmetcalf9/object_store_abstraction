from object_store_abstraction import ObjectStoreConfigError


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

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject, cacheContext):
    if not self.__isCaching():
      return None
    raise Exception("Not Implemented")

  def _saveJSONObject(self, objectType, objectKey, JSONString, objectVersion, cacheContext):
    if not self.__isCaching():
      return None
    raise Exception("Not Implemented")

  def _getObjectJSON(self, objectType, objectKey, cacheContext, mainContext):
    if not self.__isCaching():
      return mainContext._getObjectJSON(objectType, objectKey)
    raise Exception("Not Implemented")

