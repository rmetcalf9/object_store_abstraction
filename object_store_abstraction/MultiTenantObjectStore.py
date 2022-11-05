from object_store_abstraction import createObjectStoreInstance

class InvalidTenantNameException(Exception):
  pass

class MultiTenantObjectStore():
  objectStoreMap = None
  def __init__(self, objectStoreConfigDict, fns, detailLogging):
    self.objectStoreMap = {}

    if objectStoreConfigDict is None:
      raise Exception("Invalid Multitenant Object Store config - default specified but not possible for multitenant")
    if "stores" not in objectStoreConfigDict:
      raise Exception("Invalid Multitenant Object Store config - no stores")
    if len(objectStoreConfigDict["stores"]) == 0:
      raise Exception("Invalid Multitenant Object Store config - zero items in stores array")

    for curStoreConfig in objectStoreConfigDict["stores"]:
      if "tenantName" not in curStoreConfig:
        raise Exception("Invalid MULTI Object Store config - store without tenantName")
      if "config" not in curStoreConfig:
        raise Exception("Invalid MULTI Object Store config - store without config")
      cfg = curStoreConfig["config"]
      if len(list(curStoreConfig["config"].keys())) == 0:
        cfg = None
      self.objectStoreMap[curStoreConfig["tenantName"]] = createObjectStoreInstance(
        cfg,
        fns,
        detailLogging=detailLogging
      )

  def getStore(self, tenantName):
    if tenantName not in self.objectStoreMap:
      raise InvalidTenantNameException("Invalid tenant name " + tenantName)
    return self.objectStoreMap[tenantName]
