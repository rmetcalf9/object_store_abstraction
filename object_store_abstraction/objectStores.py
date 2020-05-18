from .objectStores_TenantAware import ObjectStore_TenantAware
from .objectStores_Memory import ObjectStore_Memory
from .objectStores_SQLAlchemy import ObjectStore_SQLAlchemy
from .objectStores_SimpleFileStore import ObjectStore_SimpleFileStore
from .objectStores_DynamoDB import ObjectStore_DynamoDB
from .objectStores_Migrating import ObjectStore_Migrating
from .objectStoresPackage import ObjectStore_Caching

class InvalidObjectStoreConfigMissingTypeClass(Exception):
  pass
InvalidObjectStoreConfigMissingTypeException = InvalidObjectStoreConfigMissingTypeClass('APIAPP_OBJECTSTORECONFIG value has no Type attribute')

class InvalidObjectStoreConfigUnknownTypeClass(Exception):
  pass
InvalidObjectStoreConfigUnknownTypeException = InvalidObjectStoreConfigUnknownTypeClass('APIAPP_OBJECTSTORECONFIG Type value is not recognised')

class ObjectStoreConfigNotDictObjectExceptionClass(Exception):
  pass

def _createObjectStoreInstanceTypeSpecified(type, configDICT, initFN, externalFns, detailLogging):
  print("Using Object Store Type: " + type)
  return initFN(configDICT, externalFns, detailLogging, type, createObjectStoreInstance)

def _createTenantAwareObjectStoreInstanceTypeSpecified(type, configDICT, initFN, externalFns, detailLogging):
  objectStore = _createObjectStoreInstanceTypeSpecified(type, configDICT, initFN, externalFns, detailLogging)
  return ObjectStore_TenantAware(objectStore)

#Based on applicaiton options create an instance of objectStore to be used
def createObjectStoreInstance(
  objectStoreConfigDict,
  externalFns,
  detailLogging=False,  #True or False
  tenantAware=False
):
  if 'getCurDateTime' not in externalFns:
    raise Exception("createObjectStoreInstance - Must supply getCurDateTime externalFunction")

  if objectStoreConfigDict is None:
    objectStoreConfigDict = {}
    objectStoreConfigDict["Type"] = "Memory"

  if not isinstance(objectStoreConfigDict, dict):
    raise ObjectStoreConfigNotDictObjectExceptionClass('You must pass a dict as config to createObjectStoreInstance (or None)')

  createFN = _createObjectStoreInstanceTypeSpecified
  if tenantAware:
    createFN = _createTenantAwareObjectStoreInstanceTypeSpecified

  if "Type" not in objectStoreConfigDict:
    raise InvalidObjectStoreConfigMissingTypeException
  if objectStoreConfigDict["Type"] == "Memory":
    return createFN("Memory", objectStoreConfigDict, ObjectStore_Memory, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "SQLAlchemy":
    return createFN("SQLAlchemy", objectStoreConfigDict, ObjectStore_SQLAlchemy, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "SimpleFileStore":
    return createFN("SimpleFileStore", objectStoreConfigDict, ObjectStore_SimpleFileStore, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "DynamoDB":
    return createFN("DynamoDB", objectStoreConfigDict, ObjectStore_DynamoDB, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "Migrating":
    return createFN("Migrating", objectStoreConfigDict, ObjectStore_Migrating, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "Caching":
    return createFN("Caching", objectStoreConfigDict, ObjectStore_Caching, externalFns, detailLogging)

  print("Trying to create object store type " + objectStoreConfigDict["Type"])
  raise InvalidObjectStoreConfigUnknownTypeException
