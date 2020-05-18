from object_store_abstraction import ObjectStoreConfigError
import time

class CachePolicyClass():
  caching = None
  maxqueuesize = None
  timeout = None
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
      if policy["maxCacheSize"] < 2:
        raise ObjectStoreConfigError(
          "APIAPP_OBJECTSTORECONFIG Caching ERROR - config param " + errorName + " maxCacheSize must be 2 or more")
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
    self.maxqueuesize = policy["maxCacheSize"]
    self.timeout = policy["timeout"]

  def __isCaching(self):
    return self.caching

  def __putObjectIntoCache(self, objectType, objectKey, JSONString, objectVersion, cacheContext, cullQueues):
    dictToStore = {
      "exp": time.perf_counter() + (self.timeout/1000),
      "d": JSONString
    }
    # don't supply object version otherwise it will disallow creation
    cacheContext._saveJSONObject(objectType, objectKey, dictToStore, objectVersion=None)
    queue = cullQueues.getQueue(objectType=objectType, maxsize=self.maxqueuesize)
    if queue.full():
      objectKeyFromQueue = queue.get()
      self.__removeSingleElementFromCache(cacheContext=cacheContext, objectType=objectType, objectKey=objectKeyFromQueue)
    queue.put(objectKey)

    self.__preformCull(cacheContext=cacheContext, objectType=objectType, cullQueues=cullQueues)

  def __removeSingleElementFromCache(self, cacheContext, objectType, objectKey):
    return cacheContext._removeJSONObject(objectType, objectKey, objectVersion=None, ignoreMissingObject=True)

  def __preformCull(self, cacheContext, objectType, cullQueues):
    queue = cullQueues.getQueue(objectType=objectType, maxsize=self.maxqueuesize)
    while queue.qsize() > self.maxqueuesize:
      objectKeyFromQueue = queue.get()
      self.__removeSingleElementFromCache(cacheContext=cacheContext, objectType=objectType, objectKey=objectKeyFromQueue)

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject, cacheContext):
    if not self.__isCaching():
      return None
    self.__removeSingleElementFromCache(cacheContext=cacheContext, objectType=objectType, objectKey=objectKey)
    return None # return value not used

  def _saveJSONObject(self, objectType, objectKey, JSONString, objectVersion, cacheContext, cullQueues):
    if not self.__isCaching():
      return None
    self.__putObjectIntoCache(objectType, objectKey, JSONString, objectVersion, cacheContext, cullQueues)
    return None

  def _getObjectJSON(self, objectType, objectKey, cacheContext, mainContext, cullQueues):
    if not self.__isCaching():
      return mainContext._getObjectJSON(objectType, objectKey)

    # get From Cache
    frmCacheTuple = cacheContext._getObjectJSON(objectType=objectType, objectKey=objectKey)
    if frmCacheTuple[0] is not None:

      # If it has not expired then return cache value
      if time.perf_counter() < frmCacheTuple[0]["exp"]:
        retVal = list(frmCacheTuple)
        retVal[0] = retVal[0]["d"]
        #Cache hit - we just need to return, but first make sure cache won't grow too much
        self.__preformCull(cacheContext, objectType, cullQueues)
        return tuple(retVal)
      else:
        #item has expired - remove it from cache
        self.__removeSingleElementFromCache(objectType=objectType, objectKey=objectKey)

    #We get to this point if it is not in the cache

    # If it is expired lookup from main context
    fromMainTuple = mainContext._getObjectJSON(objectType, objectKey)
    ##__putObjectIntoCache will always preform a cull to prevent too much cache growth
    self.__putObjectIntoCache(
      objectType=objectType,
      objectKey=objectKey,
      JSONString=fromMainTuple[0],
      objectVersion=fromMainTuple[1],
      cacheContext=cacheContext, cullQueues=cullQueues
    )
    return fromMainTuple


