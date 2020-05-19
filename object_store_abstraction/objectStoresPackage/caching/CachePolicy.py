from object_store_abstraction import ObjectStoreConfigError
import time
from dateutil.parser import parse
import pytz

class CachePolicyClass():
  caching = None
  maxcachesize = None
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
    self.maxcachesize = policy["maxCacheSize"]
    self.timeout = policy["timeout"]

  def __isCaching(self):
    return self.caching

  def __putObjectIntoCache(self, objectType, objectKey, JSONString, objectVersion, cacheContext, cullQueues, creationDateTime, lastUpdateDateTime):
    # We do want to store null values in the cache
    def getIsoFormatOrNone(val):
      if val is None:
        return val
      return val.isoformat()

    dictToStore = {
      "exp": time.perf_counter() + (self.timeout/1000),
      "d": JSONString,
      "ver": objectVersion,
      "cre": getIsoFormatOrNone(creationDateTime),
      "upd": getIsoFormatOrNone(lastUpdateDateTime)
    }
    #If we supply the object version then the save will fail on creation and mismatch
    # if we don't supply the object version then the save will fail if object exists
    # so instead it is looked up
    frmCacheTuple = cacheContext._getObjectJSON(objectType=objectType, objectKey=objectKey)
    if frmCacheTuple[0] is None:
      #object is not currently in cache
      cacheContext._saveJSONObjectV2(objectType, objectKey, dictToStore, objectVersion=None)
    else:
      #object in cache,
      cacheContext._saveJSONObjectV2(objectType, objectKey, dictToStore, objectVersion=frmCacheTuple[1])

    queue = cullQueues.getQueue(objectType=objectType, maxsize=self.maxcachesize)
    if queue.full():
      objectKeyFromQueue = queue.get()
      self.__removeSingleElementFromCache(cacheContext=cacheContext, objectType=objectType, objectKey=objectKeyFromQueue)
    queue.put(objectKey)

    self.__preformCull(cacheContext=cacheContext, objectType=objectType, cullQueues=cullQueues)

  def __removeSingleElementFromCache(self, cacheContext, objectType, objectKey):
    return cacheContext._removeJSONObject(objectType, objectKey, objectVersion=None, ignoreMissingObject=True)

  def __preformCull(self, cacheContext, objectType, cullQueues):
    queue = cullQueues.getQueue(objectType=objectType, maxsize=self.maxcachesize)
    while queue.qsize() > self.maxcachesize:
      objectKeyFromQueue = queue.get()
      self.__removeSingleElementFromCache(cacheContext=cacheContext, objectType=objectType, objectKey=objectKeyFromQueue)

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject, cacheContext):
    if not self.__isCaching():
      return None
    self.__removeSingleElementFromCache(cacheContext=cacheContext, objectType=objectType, objectKey=objectKey)
    return None # return value not used

  def _saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion, cacheContext, cullQueues, creationDateTime, lastUpdateDateTime):
    if not self.__isCaching():
      return None
    self.__putObjectIntoCache(
      objectType,
      objectKey,
      JSONString,
      objectVersion,
      cacheContext,
      cullQueues,
      creationDateTime=creationDateTime,
      lastUpdateDateTime=lastUpdateDateTime
    )
    return None

  def _getObjectJSON(self, objectType, objectKey, cacheContext, mainContext, cullQueues):
    if not self.__isCaching():
      return mainContext._getObjectJSON(objectType, objectKey)

    # get From Cache
    frmCacheTuple = cacheContext._getObjectJSON(objectType=objectType, objectKey=objectKey)
    if frmCacheTuple[0] is not None:

      def getOutputDateTime(isoforamtDateTime):
        if isoforamtDateTime is None:
          return None
        dt = parse(isoforamtDateTime)
        return dt.astimezone(pytz.utc)

      # If it has not expired then return cache value
      if time.perf_counter() < frmCacheTuple[0]["exp"]:
        retVal = list(frmCacheTuple)
        retVal[0] = frmCacheTuple[0]["d"]
        retVal[1] = frmCacheTuple[0]["ver"] #speficy original object version
        retVal[2] = getOutputDateTime(frmCacheTuple[0]["cre"]) #speficy original (May not always ve correct)
        retVal[3] = getOutputDateTime(frmCacheTuple[0]["upd"]) #speficy original (May not always ve correct)
        #Cache hit - we just need to return, but first make sure cache won't grow too much
        self.__preformCull(cacheContext, objectType, cullQueues)
        return tuple(retVal)
      else:
        #item has expired - remove it from cache
        self.__removeSingleElementFromCache(cacheContext=cacheContext, objectType=objectType, objectKey=objectKey)

    #We get to this point if it is not in the cache

    # If it is expired lookup from main context
    fromMainTuple = mainContext._getObjectJSON(objectType, objectKey)

    ##__putObjectIntoCache will always preform a cull to prevent too much cache growth
    self.__putObjectIntoCache(
      objectType=objectType,
      objectKey=objectKey,
      JSONString=fromMainTuple[0],
      objectVersion=fromMainTuple[1],
      cacheContext=cacheContext,
      cullQueues=cullQueues,
      creationDateTime=fromMainTuple[2],
      lastUpdateDateTime=fromMainTuple[3]
    )
    return fromMainTuple


