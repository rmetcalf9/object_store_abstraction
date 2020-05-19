
objectTypeSeperator = "$:}wol{}"

class CallingNonTenantAwareVersion(Exception):
  pass
CallingNonTenantAwareVersionException = CallingNonTenantAwareVersion('Tried to call a non-tenant aware version')
class UnsupportedTenantName(Exception):
  pass
UnsupportedTenantNameException = UnsupportedTenantName('Tenant Name contains a reserved word')
class UnsupportedObjectType(Exception):
  pass
UnsupportedObjectTypeException = UnsupportedObjectType('Object Type contains a reserved word')

class TenantAwareConnectionContext():
  objectStoreContext = None
  tenantName = None
  def __init__(self, tenantName, objectStoreContext):
    self.tenantName = tenantName
    self.objectStoreContext = objectStoreContext
    if objectTypeSeperator in tenantName:
      raise UnsupportedTenantNameException

  def __INT__getDirivedObjectType(self, objectType):
    if objectTypeSeperator in objectType:
      raise UnsupportedObjectTypeException
    return self.tenantName + objectTypeSeperator + objectType

  def executeInsideTransaction(self, fnToExecute):
    retVal = None
    self.objectStoreContext._INT_startTransaction()
    try:
      retVal = fnToExecute(self)
      self.objectStoreContext._INT_commitTransaction()
    except:
      self.objectStoreContext._INT_rollbackTransaction()
      raise
    return retVal


  def saveJSONObject(self, objectType, objectKey, JSONString, objectVersion = None):
    return self.objectStoreContext.saveJSONObject(self.__INT__getDirivedObjectType(objectType), objectKey, JSONString, objectVersion)

  def saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion = None):
    return self.objectStoreContext.saveJSONObjectV2(self.__INT__getDirivedObjectType(objectType), objectKey, JSONString, objectVersion)

  def removeJSONObject(self, objectType, objectKey, objectVersion = None, ignoreMissingObject = False):
    return self.objectStoreContext.removeJSONObject(self.__INT__getDirivedObjectType(objectType), objectKey, objectVersion, ignoreMissingObject)

  def updateJSONObject(self, objectType, objectKey, updateFn, objectVersion = None):
    return self.objectStoreContext.updateJSONObject(self.__INT__getDirivedObjectType(objectType), objectKey, updateFn, objectVersion)

  def getObjectJSON(self, objectType, objectKey):
    return self.objectStoreContext.getObjectJSON(self.__INT__getDirivedObjectType(objectType), objectKey)

  def getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    return self.objectStoreContext.getPaginatedResult(self.__INT__getDirivedObjectType(objectType), paginatedParamValues, outputFN)

  def getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    return self.objectStoreContext.getAllRowsForObjectType(self.__INT__getDirivedObjectType(objectType), filterFN, outputFN, whereClauseText)

  def list_all_objectTypes(self):
    res = []
    resAllTenants = self.objectStoreContext.list_all_objectTypes()
    stcheck = self.tenantName + objectTypeSeperator
    for x in resAllTenants:
      if x.startswith(stcheck):
        res.append(x[len(stcheck):])
    return res

  def _close(self):
    return self.objectStoreContext._close()


class ObjectStore_TenantAware():
  objectStore = None
  def __init__(self, objectStore):
    self.objectStore = objectStore

  def detailLog(self, logMsg):
    return self.objectStore.detailLog(logMsg)

  #def getConnectionContext(self):
  #  raise CallingNonTenantAwareVersionException

  #def executeInsideConnectionContext(self, fnToExecute):
  #  raise CallingNonTenantAwareVersionException

  #helper function if we need just a single transaction in our contexts
  #def executeInsideTransaction(self, fnToExecute):
  #  raise CallingNonTenantAwareVersionException

  def resetDataForTest(self):
    return self.objectStore.resetDataForTest()

  def getConnectionContext(self, tenantName):
    context = self.objectStore.getConnectionContext()
    return TenantAwareConnectionContext(tenantName, context)

  def executeInsideConnectionContext(self, tenantName, fnToExecute):
    context = self.getConnectionContext(tenantName)
    a = None
    try:
      a = fnToExecute(context)
    finally:
      context._close()
    return a

  #helper function if we need just a single transaction in our contexts
  def executeInsideTransaction(self, tenantName, fnToExecute):
    def dbfn(context):
      return context.executeInsideTransaction(fnToExecute)
    return self.executeInsideConnectionContext(tenantName, dbfn)
