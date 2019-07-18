#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item

from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, ObjectStoreConfigError
from .paginatedResult import getPaginatedResult

from boto3 import Session as AWSSession
from botocore.config import Config
from botocore import exceptions as botocoreexceptions
import logging
from dateutil.parser import parse
import pytz
from .makeDictJSONSerializable import getJSONtoPutInStore, getObjFromJSONThatWasPutInStore

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

  def _saveJSONObject(self, objectType, objectKey, JSONString, objectVersion):
    (curObjectDICT, curObjectVersion, curCreationDate, curLastUpdateDate) = self._getObjectJSON(objectType, objectKey)
    curTime = self.objectStore.externalFns['getCurDateTime']().isoformat()

    if curObjectDICT is None:
      if objectVersion is not None:
        raise SuppliedObjectVersionWhenCreatingException
      newObjectVersion = 1

      response = self.objectStore.dynTable.put_item(
         Item={
              'objectType': objectType,
              'objectKey': objectKey,
              'objectVersion': newObjectVersion,
              'creationDate': curTime,
              'lastUpdateData': curTime,
              'objectDICT': getJSONtoPutInStore(JSONString)
          }
      )
      return newObjectVersion

    if objectVersion is None:
      raise TryingToCreateExistingObjectException
    if str(curObjectVersion) != str(objectVersion):
      raise WrongObjectVersionException
    newObjectVersion = curObjectVersion + 1

    response = self.objectStore.dynTable.update_item(
        Key={
            'objectType': objectType,
            'objectKey': objectKey
        },
        UpdateExpression="set objectVersion = :ov, lastUpdateData=:lud, objectDICT=:od",
        ExpressionAttributeValues={
            ':ov': newObjectVersion,
            ':lud': curTime,
            ':od': getJSONtoPutInStore(JSONString)
        },
        ReturnValues="UPDATED_NEW"
    )
    return newObjectVersion

  def __getTupleFromItem(self, item):
    dt = parse(item['creationDate'])
    creationDate = dt.astimezone(pytz.utc)
    dt = parse(item['lastUpdateData'])
    lastUpdateDate = dt.astimezone(pytz.utc)
    convertedObjectDICT = getObjFromJSONThatWasPutInStore(item['objectDICT'])

    return convertedObjectDICT, item['objectVersion'], creationDate, lastUpdateDate

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    #Object version error only raised when not ignoring missing
    #https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html#Expressions.ConditionExpressions.AdvancedComparisons
    ConditionalExpression = None
    ExpressionAttributeValues = None
    if not ignoreMissingObject:
      ConditionalExpression = '(objectType = :objectType) and (objectKey = :objectKey)'
      ExpressionAttributeValues={
        ':objectType': objectType,
        ':objectKey': objectKey,
      }
    try:
      if ConditionalExpression is None:
        response = self.objectStore.dynTable.delete_item(
          Key={
            'objectType': objectType,
            'objectKey': objectKey
          }
        )
      else:
        response = self.objectStore.dynTable.delete_item(
          Key={
            'objectType': objectType,
            'objectKey': objectKey
          },
          ConditionExpression=ConditionalExpression,
          ExpressionAttributeValues=ExpressionAttributeValues
        )
    except botocoreexceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        raise TriedToDeleteMissingObjectException


  #Return value is objectDICT, ObjectVersion, creationDate, lastUpdateDate
  #Return None, None, None, None if object isn't in store
  def _getObjectJSON(self, objectType, objectKey):

    response = self.objectStore.dynTable.get_item(
        Key={
            'objectType': objectType,
            'objectKey': objectKey
        }
    )
    if "Item" not in response:
      return None, None, None, None

    return self.__getTupleFromItem(response["Item"])


  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html?highlight=delete_item#paginators
    raise Exception('TODO override dynmodb _getPaginatedResult')

  def _getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    raise Exception('TODO override dynmodb _getAllRowsForObjectType')



# Class that will store objects
class ObjectStore_DynamoDB(ObjectStore):
  awsSession = None
  awsDynamodbClient = None
  objectPrefix = None
  dynTable = None

  def __init__(self, configJSON, externalFns, detailLogging, type):
    super(ObjectStore_DynamoDB, self).__init__(externalFns, detailLogging, type)

    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('nose').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

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

    self.awsDynamodbClient = self.awsSession.resource('dynamodb', region_name=configJSON["region_name"], endpoint_url=endpointURL, config=config)

    #This could prbally be more efficient
    tblExists = False
    for x in self.awsDynamodbClient.tables.all():
      if x.name == self.objectPrefix + '_objData':
        tblExists = True

    if not tblExists:
      self.__createTable()

    #existing_tables = self.awsDynamodbClient.list_tables()['TableNames']
    #if self.objectPrefix + '_objData' not in existing_tables:
    #  self.__createTable()

    self.dynTable = self.awsDynamodbClient.Table(self.objectPrefix + '_objData')


  def __createTable(self):
    resp = self.awsDynamodbClient.create_table(
        TableName=self.objectPrefix + '_objData',
        # Declare your Primary Key in the KeySchema argument
        KeySchema=[
            {
                "AttributeName": "objectType",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "objectKey",
                "KeyType": "RANGE"
            }
        ],
        # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
        AttributeDefinitions=[
            {
                "AttributeName": "objectType",
                "AttributeType": "S"
            },
            {
                "AttributeName": "objectKey",
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
      self.dynTable.delete()
      #resp = self.awsDynamodbClient.delete_table(
      #    TableName=self.objectPrefix + '_objData'
      #)
      self.__createTable()
      self.dynTable = self.awsDynamodbClient.Table(self.objectPrefix + '_objData')
    self.executeInsideTransaction(someFn)

  def _getConnectionContext(self):
    return ConnectionContext(self)
