from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, ObjectStoreConfigError
from .paginatedResult import getPaginatedResult

from boto3 import Session as AWSSession

class ConnectionContext(ObjectStoreConnectionContext):
  objectStore = None
  def __init__(self, objectStore):
    super(ConnectionContext, self).__init__()
    self.objectStore = objectStore

  #transactions not implemented
  def _startTransaction(self):
    pass
  def _commitTransaction(self):
    pass
  def _rollbackTransaction(self):
    pass

# Class that will store objects
class ObjectStore_DynamoDB(ObjectStore):
  awsSession = None
  objectPrefix = None

  def __init__(self, configJSON, externalFns, detailLogging, type):
    super(ObjectStore_DynamoDB, self).__init__(externalFns, detailLogging, type)

    requiredConfigItems = ['aws_access_key_id','aws_secret_access_key','region_name','endpoint_url']
    for x in requiredConfigItems:
      if x not in configJSON:
        raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG DynamoDB ERROR - config param " + x + " missing")
    endpointURL = configJSON["endpoint_url"]
    if endpointURL.strip().upper() == "NONE":
      endpointURL = None

    self.awsSession = AWSSession(
      aws_access_key_id=configJSON["aws_access_key_id"],
      aws_secret_access_key=configJSON["aws_secret_access_key"],
      aws_session_token=None,
      region_name=configJSON["region_name"],
      botocore_session=None,
      profile_name=None
    )
    if "objectPrefix" in ConfigDict:
      self.objectPrefix = ConfigDict["objectPrefix"]
    else:
      self.objectPrefix = ""


    #Dict = (objDICT, objectVersion, creationDate, lastUpdateDate)

  def _resetDataForTest(self):
    def someFn(connectionContext):
      raise Exception("TODO Reset data for test")
    self.executeInsideTransaction(someFn)

  def _getConnectionContext(self):
    return ConnectionContext(self)
