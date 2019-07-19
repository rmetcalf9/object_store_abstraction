#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item

from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException, ObjectStoreConfigError
from .paginatedResult import getPaginatedResult

from boto3 import Session as AWSSession
from botocore.config import Config
from botocore import exceptions as botocoreexceptions
from boto3.dynamodb.conditions import Key, Attr

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

      jsonToStore = getJSONtoPutInStore(JSONString)
      response = self.objectStore.getTable(objectType).put_item(
         Item={
              'objectType': objectType,
              'objectKey': objectKey,
              'objectVersion': newObjectVersion,
              'creationDate': curTime,
              'lastUpdateData': curTime,
              'objectDICT': jsonToStore
          }
      )
      return newObjectVersion

    if objectVersion is None:
      raise TryingToCreateExistingObjectException
    if str(curObjectVersion) != str(objectVersion):
      raise WrongObjectVersionException
    newObjectVersion = curObjectVersion + 1

    jsonToStore = getJSONtoPutInStore(JSONString)
    response = self.objectStore.getTable(objectType).update_item(
        Key={
            'objectType': objectType,
            'objectKey': objectKey
        },
        UpdateExpression="set objectVersion = :ov, lastUpdateData=:lud, objectDICT=:od",
        ExpressionAttributeValues={
            ':ov': newObjectVersion,
            ':lud': curTime,
            ':od': jsonToStore
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
        response = self.objectStore.getTable(objectType).delete_item(
          Key={
            'objectType': objectType,
            'objectKey': objectKey
          }
        )
      else:
        response = self.objectStore.getTable(objectType).delete_item(
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

    response = self.objectStore.getTable(objectType).get_item(
        Key={
            'objectType': objectType,
            'objectKey': objectKey
        }
    )
    if "Item" not in response:
      return None, None, None, None

    return self.__getTupleFromItem(response["Item"])


  def __getAllRowsForObjectType(self, objectType, offset, pagesize):
    response = {'LastEvaluatedKey': 'setToStartLoop'}
    srcData = {}
    numFetched = 0
    fetching = True
    while ('LastEvaluatedKey' in response) and (fetching == True):
      response = self.objectStore.getTable(objectType).query(
          KeyConditionExpression=Key('objectType').eq(objectType),
      )
      for curItem in response['Items']:
        if fetching:
          numFetched = numFetched + 1
          srcData[curItem['objectKey']] = self.__getTupleFromItem(curItem)
          if offset != None: # allows this to work in non-pagination mode
            if numFetched > (offset + pagesize): #Total caculation will be off when we don't go thorough entire dataset
                                # but invalid figure will be always be one over as we fetch one past in all cases
              fetching = False
    return srcData

  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html?highlight=delete_item#paginators
    srcData = self.__getAllRowsForObjectType(objectType, paginatedParamValues['offset'], paginatedParamValues['pagesize'])

    #paginatedParamValues['query'],
    #paginatedParamValues['offset'],
    #paginatedParamValues['pagesize']

    return getPaginatedResult(
      list=srcData,
      outputFN=outputFN,
      offset=paginatedParamValues['offset'],
      pagesize=paginatedParamValues['pagesize'],
      query=paginatedParamValues['query'],
      sort=paginatedParamValues['sort'],
      filterFN=self._filterFN_basicTextInclusion
    )

  def _getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    superObj = self.__getAllRowsForObjectType(objectType, None, None)
    outputLis = []
    for curKey in superObj:
      if self._filterFN_basicTextInclusion(superObj[curKey], whereClauseText):
        if filterFN(superObj[curKey], whereClauseText):
          outputLis.append(superObj[curKey])
    return list(map(outputFN, outputLis))

# Class that will store objects
class ObjectStore_DynamoDB(ObjectStore):
  awsSession = None
  awsDynamodbClient = None
  objectPrefix = None
  dynTables = None
  singleTableMode = False

  #Return a table for an objectype, creating it if required
  def getTable(self, objectType):
    if self.singleTableMode:
      return self.__getTable('xx')
    return self.__getTable(objectType)

  def __getTable(self, objectType):
    if objectType not in self.dynTables:
      self.__createTable(objectType)
    return self.dynTables[objectType]['dyn']

  def getTableName(self, objectType):
    return self.objectPrefix + '_objData_' + objectType

  def __init__(self, configJSON, externalFns, detailLogging, type):
    super(ObjectStore_DynamoDB, self).__init__(externalFns, detailLogging, type)

    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('nose').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    requiredConfigItems = ['aws_access_key_id','aws_secret_access_key','region_name','endpoint_url', 'single_table_mode']
    for x in requiredConfigItems:
      if x not in configJSON:
        raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG DynamoDB ERROR - config param " + x + " missing")
    endpointURL = configJSON["endpoint_url"]
    if endpointURL.strip().upper() == "NONE":
      endpointURL = None

    singleTableModeTT = configJSON["single_table_mode"].strip().upper()
    if singleTableModeTT == "FALSE":
      self.singleTableMode = False
    else:
      if singleTableModeTT.strip().upper() == "TRUE":
        self.singleTableMode = True
      else:
        raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG DynamoDB ERROR - single_table_mode must be true or false")


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

    self.dynTables = {}
    for x in self.awsDynamodbClient.tables.all():
      if x.name.startswith(self.objectPrefix + '_objData_'):
        objectType = x.name[len(self.objectPrefix + '_objData_'):]
        self.dynTables[objectType] = {
          'name': objectType,
          'dyn': self.awsDynamodbClient.Table(self.getTableName(objectType))
        }


  def __createTable(self, objectType):
    resp = self.awsDynamodbClient.create_table(
        TableName=self.getTableName(objectType),
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
    self.dynTables[objectType] = {
      'name': objectType,
      'dyn': self.awsDynamodbClient.Table(self.getTableName(objectType))
    }


    #Dict = (objDICT, objectVersion, creationDate, lastUpdateDate)

  def _resetDataForTest(self):
    def someFn(connectionContext):
      for objectType in self.dynTables:
        self.dynTables[objectType]['dyn'].delete()
      self.dynTables = {}
    self.executeInsideTransaction(someFn)

  def _getConnectionContext(self):
    return ConnectionContext(self)
