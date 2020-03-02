# A version of Repository that caches the objects instead of retrieveing on every get
# provides a reset which clears cache and forces gets to happen again
from .RepositoryBaseClass import RepositoryBaseClass


class RepositoryCachingBaseClass(RepositoryBaseClass):
  cachedObjects = None
  def __init__(self, objectStoreTypeString, objectFactoryFn=None):
    RepositoryBaseClass.__init__(self, objectStoreTypeString, objectFactoryFn)
    self.resetCache()

  def resetCache(self):
    self.cachedObjects = {}

  def resetCacheItem(self, id):
    del self.cachedObjects[id]

  def upsert(self, obj, objectVersion, storeConnection):
    # in upsert obj is ALWAYS the dict so we can not update the object
    # we do not remove it from the cache to force a relaod - if the caller wants that
    # they can use resetCacheItem call
    return RepositoryBaseClass.upsert(self, obj, objectVersion, storeConnection)

  def get(self, id, storeConnection):
    if id in self.cachedObjects:
      return self.cachedObjects[id]
    obj = RepositoryBaseClass.get(self, id, storeConnection)
    if obj is not None: # don't add to cache if it was not found
      self.cachedObjects[id] = obj
    return obj

  def remove(self, id, storeConnection):
    self.resetCacheItem(id)
    return RepositoryBaseClass.remove(self, id, storeConnection)
