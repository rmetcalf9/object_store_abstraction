from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, ObjectStoreConfigError
from .paginatedResult import getPaginatedResult

from boto3 import Session as AWSSession
from botocore.config import Config

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
  awsDynamodbClient = None
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
    if "objectPrefix" in configJSON:
      self.objectPrefix = configJSON["objectPrefix"]
    else:
      self.objectPrefix = ""

    config = Config(
        connect_timeout=1, read_timeout=1,
        retries={'max_attempts': 1})

    self.awsDynamodbClient = self.awsSession.client('dynamodb', region_name=configJSON["region_name"], endpoint_url=endpointURL, config=config)

    existing_tables = self.awsDynamodbClient.list_tables()['TableNames']
    if self.objectPrefix + '_objData' not in existing_tables:
      self.__createTable()


  def __createTable(self):
    resp = self.awsDynamodbClient.create_table(
        TableName=self.objectPrefix + '_objData',
        # Declare your Primary Key in the KeySchema argument
        KeySchema=[
            {
                "AttributeName": "type",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "key",
                "KeyType": "RANGE"
            }
        ],
        # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
        AttributeDefinitions=[
            {
                "AttributeName": "type",
                "AttributeType": "S"
            },
            {
                "AttributeName": "key",
                "AttributeType": "S"
            }
        ],
        # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
        # You can control read and write capacity independently.
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
        }
    )


    #Dict = (objDICT, objectVersion, creationDate, lastUpdateDate)

  def _resetDataForTest(self):
    def someFn(connectionContext):
      resp = self.awsDynamodbClient.delete_table(
          TableName=self.objectPrefix + '_objData'
      )
      self.__createTable()
    self.executeInsideTransaction(someFn)

  def _getConnectionContext(self):
    return ConnectionContext(self)
