from .objectStores_Memory import ObjectStore_Memory
from .objectStores_SQLAlchemy import ObjectStore_SQLAlchemy
from .objectStores_SimpleFileStore import ObjectStore_SimpleFileStore
from .objectStores_DynamoDB import ObjectStore_DynamoDB
from .objectStores_Migrating import ObjectStore_Migrating

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

#Based on applicaiton options create an instance of objectStore to be used
def createObjectStoreInstance(
  objectStoreConfigDict,
  externalFns,
  detailLogging=False  #True or False
):
  if 'getCurDateTime' not in externalFns:
    raise Exception("createObjectStoreInstance - Must supply getCurDateTime externalFunction")

  if objectStoreConfigDict is None:
    objectStoreConfigDict = {}
    objectStoreConfigDict["Type"] = "Memory"

  if not isinstance(objectStoreConfigDict, dict):
    raise ObjectStoreConfigNotDictObjectExceptionClass('You must pass a dict as config to createObjectStoreInstance (or None)')


  if "Type" not in objectStoreConfigDict:
    raise InvalidObjectStoreConfigMissingTypeException
  if objectStoreConfigDict["Type"] == "Memory":
    return _createObjectStoreInstanceTypeSpecified("Memory", objectStoreConfigDict, ObjectStore_Memory, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "SQLAlchemy":
    return _createObjectStoreInstanceTypeSpecified("SQLAlchemy", objectStoreConfigDict, ObjectStore_SQLAlchemy, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "SimpleFileStore":
    return _createObjectStoreInstanceTypeSpecified("SimpleFileStore", objectStoreConfigDict, ObjectStore_SimpleFileStore, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "DynamoDB":
    return _createObjectStoreInstanceTypeSpecified("DynamoDB", objectStoreConfigDict, ObjectStore_DynamoDB, externalFns, detailLogging)
  if objectStoreConfigDict["Type"] == "Migrating":
    return _createObjectStoreInstanceTypeSpecified("Migrating", objectStoreConfigDict, ObjectStore_Migrating, externalFns, detailLogging)

  print("Trying to create object store type " + objectStoreConfigDict["Type"])
  raise InvalidObjectStoreConfigUnknownTypeException
