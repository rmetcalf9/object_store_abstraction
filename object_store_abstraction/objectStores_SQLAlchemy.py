from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, ObjectStoreConfigError, MissingTransactionContextException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, DateTime, JSON, func, UniqueConstraint, and_, Text
import pytz
##import datetime
from dateutil.parser import parse
import os
from .paginatedResult import getPaginatedResult

from .makeDictJSONSerializable import getJSONtoPutInStore, getObjFromJSONThatWasPutInStore


###---------- Code to get actual query being run
'''
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType

PY3 = str is not bytes
text = str if PY3 else unicode
int_type = int if PY3 else (int, long)
str_type = str if PY3 else (str, unicode)
class StringLiteral(String):
    """Teach SA how to literalize various things."""
    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, int_type):
                return text(value)
            if not isinstance(value, str_type):
                value = text(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result
        return process


class LiteralDialect(DefaultDialect):
    colspecs = {
        # prevent various encoding explosions
        String: StringLiteral,
        # teach SA about how to literalize a datetime
        DateTime: StringLiteral,
        # don't format py2 long integers to NULL
        NullType: StringLiteral,
    }


def literalquery(statement):
    """NOTE: This is entirely insecure. DO NOT execute the resulting strings."""
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        statement = statement.statement
    return statement.compile(
        dialect=LiteralDialect(),
        compile_kwargs={'literal_binds': True},
    ).string
'''
###--------------

objectStoreHardCodedVersionInteger = 1

class ConnectionContext(ObjectStoreConnectionContext):
  connection = None
  transaction = None
  objectStore = None

  def __init__(self, objectStore):
    super(ConnectionContext, self).__init__()
    self.objectStore = objectStore
    self.connection = self.objectStore.engine.connect()

  def _startTransaction(self):
    if self.transaction is not None:
      raise Exception("ERROR Starting transaction when there is already one in progress")
    self.transaction = self.connection.begin()

  #Internal function for executing a statement
  ## only called from this file
  def _INT_execute(self, statement):
    if self.transaction is None:
      MissingTransactionContextException
    return self.connection.execute(statement.execution_options(autocommit=False))

  def _commitTransaction(self):
    res = self.transaction.commit()
    self.transaction = None
    return res
  def _rollbackTransaction(self):
    res = self.transaction.rollback()
    self.transaction = None
    return res

  def _saveJSONObject(self, objectType, objectKey, JSONString, objectVersion):
    #print("JSONString:", JSONString)
    query = self.objectStore.objDataTable.select(
      whereclause=(
        and_(
          self.objectStore.objDataTable.c.type==objectType,
          self.objectStore.objDataTable.c.key==objectKey
          )
        )
    )
    result =  self._INT_execute(query)
    firstRow = result.first()
    #print("_saveJSONObject:" + objectType + ":" + objectKey + ":", objectVersion)
    #if firstRow is not None:
    #  print(" firstRow:", firstRow)
    curTime = self.objectStore.externalFns['getCurDateTime']()
    if firstRow is None:
      if objectVersion is not None:
        raise SuppliedObjectVersionWhenCreatingException
      newObjectVersion = 1
      query = self.objectStore.objDataTable.insert().values(
        type=objectType,
        key=objectKey,
        objectVersion=newObjectVersion,
        objectDICT=getJSONtoPutInStore(JSONString),
        creationDate=curTime,
        lastUpdateDate=curTime,
        creationDate_iso8601=curTime.isoformat(),
        lastUpdateDate_iso8601=curTime.isoformat()
      )
      result = self._INT_execute(query)
      if len(result.inserted_primary_key) != 1:
        raise Exception('_saveJSONObject wrong number of rows inserted')
      #if result.inserted_primary_key[0] != objectKey:
      #  raise Exception('_saveJSONObject issue with primary key')
      return newObjectVersion
    if objectVersion is None:
      raise TryingToCreateExistingObjectException
    if str(firstRow.objectVersion) != str(objectVersion):
      raise WrongObjectVersionException
    newObjectVersion = firstRow.objectVersion + 1
    query = self.objectStore.objDataTable.update(whereclause=(
      and_(
        self.objectStore.objDataTable.c.type==objectType,
        self.objectStore.objDataTable.c.key==objectKey
        )
      )).values(
      objectVersion=newObjectVersion,
      objectDICT=getJSONtoPutInStore(JSONString),
      lastUpdateDate=curTime,
      lastUpdateDate_iso8601=curTime.isoformat()
    )
    result = self._INT_execute(query)
    if result.rowcount != 1:
      print('Result count is ', result.rowcount)
      raise Exception('_saveJSONObject wrong number of rows updated')
    return newObjectVersion

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    query = self.objectStore.objDataTable.delete(whereclause=(
      and_(
        self.objectStore.objDataTable.c.type==objectType,
        self.objectStore.objDataTable.c.key==objectKey
      )
    ))
    result = self._INT_execute(query)
    if result.rowcount == 0:
      if not ignoreMissingObject:
        raise TriedToDeleteMissingObjectException

  def _INT_getTupleFromRow(self, row):
    dt = parse(row['creationDate_iso8601'])
    creationDate = dt.astimezone(pytz.utc)
    dt = parse(row['lastUpdateDate_iso8601'])
    lastUpdateDate = dt.astimezone(pytz.utc)
    convertedObjectDICT = getObjFromJSONThatWasPutInStore(row['objectDICT'])

    return convertedObjectDICT, row['objectVersion'], creationDate, lastUpdateDate


  #Return value is objectDICT, ObjectVersion, creationDate, lastUpdateDate
  #Return None, None, None, None if object isn't in store
  ObjTableKeyMap = None
  def _getObjectJSON(self, objectType, objectKey):
    query = self.objectStore.objDataTable.select(whereclause=(
      and_(
        self.objectStore.objDataTable.c.type==objectType,
        self.objectStore.objDataTable.c.key==objectKey
      )
    ))
    result = self._INT_execute(query)
    firstRow = result.fetchone()
    if firstRow is None:
      return None, None, None, None
    if result.rowcount != 1:
      raise Exception('_getObjectJSON Wrong number of rows returned for key')

    return self._INT_getTupleFromRow(firstRow)

  def _INT_filterFn(self, item, whereClauseText):
    return True

  def __getObjectTypeListFromDBUsingQuery(self, objectType, queryString, offset, pagesize):
    whereclauseToUse = self.objectStore.objDataTable.c.type==objectType
    if queryString is not None:
      if queryString != '':
        whereclauseToUse = and_(
          whereclauseToUse,
          self.objectStore.objDataTable.c.objectDICT.ilike('%' + queryString + '%')
        )
    query = self.objectStore.objDataTable.select(
      whereclause=whereclauseToUse,
      order_by=self.objectStore.objDataTable.c.key
    )
    '''
     SELECT "_objData".id, "_objData".type, "_objData".key, "_objData"."objectDICT", "_objData"."objectVersion", "_objData"."creationDate", "_objData"."lastUpdateDate", "_objData"."creationDate_iso8601", "_objData"."lastUpdateDate_iso8601"
     FROM "_objData"
     WHERE "_objData".type = 'Test1' AND lower("_objData"."objectDICT") LIKE lower('%''AA'': 3%') ORDER BY "_objData".key
    '''

    #print("\n---------\nwhereclauseToUse:", whereclauseToUse)
    #print("query:", literalquery(query))
    result =  self._INT_execute(query)

    srcData = {}
    fetching = True
    numFetched = 0
    while (fetching):
      row = result.fetchone()
      numFetched = numFetched + 1
      if row is None:
        fetching = False
      else:
        srcData[row['key']] = self._INT_getTupleFromRow(row)
      if offset != None: # allows this to work in non-pagination mode
        if numFetched > (offset + pagesize): #Total caculation will be off when we don't go thorough entire dataset
                            # but invalid figure will be always be one over as we fetch one past in all cases
          fetching = False
    return srcData

  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    srcData = self.__getObjectTypeListFromDBUsingQuery(
      objectType,
      paginatedParamValues['query'],
      paginatedParamValues['offset'],
      paginatedParamValues['pagesize']
    )

    return getPaginatedResult(
      list=srcData,
      outputFN=outputFN,
      offset=paginatedParamValues['offset'],
      pagesize=paginatedParamValues['pagesize'],
      query=paginatedParamValues['query'],
      sort=paginatedParamValues['sort'],
      filterFN=self._INT_filterFn
    )

  def _getAllRowsForObjectType(self, objectType, filterFN, outputFN, whereClauseText):
    superObj = self.__getObjectTypeListFromDBUsingQuery(
      objectType,
      queryString = whereClauseText,
      offset = None,
      pagesize = None
    )
    outputLis = []
    for curKey in superObj:
      if filterFN(superObj[curKey], ''):
        outputLis.append(superObj[curKey])
    return list(map(outputFN, outputLis))


  def _close(self):
    self.connection.close()


class ObjectStore_SQLAlchemy(ObjectStore):
  engine = None
  objDataTable = None
  verTable = None
  objectPrefix = None
  def __init__(self, ConfigDict, externalFns):
    super(ObjectStore_SQLAlchemy, self).__init__(externalFns)
    if "connectionString" not in ConfigDict:
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SQLAlchemy ERROR - Expected connectionString")
    if "objectPrefix" in ConfigDict:
      self.objectPrefix = ConfigDict["objectPrefix"]
    else:
      self.objectPrefix = ""

    connect_args = None
    if "ssl_ca" in ConfigDict:
      print("ssl_ca:", ConfigDict['ssl_ca'])
      if not os.path.isfile(ConfigDict['ssl_ca']):
        raise Exception("Supplied ssl_ca dosen't exist")
      connect_args = {
        "ssl": {'ca': ConfigDict['ssl_ca']}
      }

    #My experiment for SSL https://code.metcarob.com/node/249
    #debugging https://github.com/PyMySQL/PyMySQL/blob/master/pymysql/connections.py

    if connect_args is None:
      self.engine = create_engine(ConfigDict["connectionString"], pool_recycle=3600, pool_size=40, max_overflow=0)
    else:
      self.engine = create_engine(ConfigDict["connectionString"], pool_recycle=3600, pool_size=40, max_overflow=0, connect_args=connect_args)

    metadata = MetaData()
    #(objDICT, objectVersion, creationDate, lastUpdateDate)
    #from https://stackoverflow.com/questions/15157227/mysql-varchar-index-length
    #MySQL assumes 3 bytes per utf8 character. 255 characters is the maximum index size you can specify per column, because 256x3=768, which breaks the 767 byte limit.
    self.objDataTable = Table(self.objectPrefix + '_objData', metadata,
        #Tired intorudcing a seperate primary key and using key column as index but
        # I found the same lenght restriction exists on an index
        Column('id', Integer, primary_key=True),
        Column('type', String(50), index=True),
        Column('key', String(140), index=True), #MariaDB has smaller limit on inexes
        #Column('objectDICT', JSON), #MariaDB has not implemented JSON data type
        Column('objectDICT', Text),
        Column('objectVersion', BigInteger),
        Column('creationDate', DateTime(timezone=True)),
        Column('lastUpdateDate', DateTime(timezone=True)),
        Column('creationDate_iso8601', String(length=40)),
        Column('lastUpdateDate_iso8601', String(length=40)),
        UniqueConstraint('type', 'key', name=self.objectPrefix + '_objData_ix1')
    )
    self.verTable = Table(self.objectPrefix + '_ver', metadata,
        Column('id', Integer, primary_key=True),
        Column('first_installed_ver', Integer),
        Column('current_installed_ver', Integer),
        Column('creationDate_iso8601', String(length=40)),
        Column('lastUpdateDate_iso8601', String(length=40))
    )
    metadata.create_all(self.engine)

    self._INT_setupOrUpdateVer(externalFns)

  #AppObj passed in as None
  def _INT_setupOrUpdateVer(self, externalFns):
    def someFn(connectionContext):
      curTime = externalFns['getCurDateTime']()
      query = self.verTable.select()
      result = connectionContext._INT_execute(query)
      if result.rowcount != 1:
        if result.rowcount != 0:
          raise Exception('invalid database structure - can\'t read version')
        #There are 0 rows, create one
        query = self.verTable.insert().values(
          first_installed_ver=objectStoreHardCodedVersionInteger,
          current_installed_ver=objectStoreHardCodedVersionInteger,
          creationDate_iso8601=curTime.isoformat(),
          lastUpdateDate_iso8601=curTime.isoformat()
        )
        result = connectionContext._INT_execute(query)
        return
      firstRow = result.first()
      if objectStoreHardCodedVersionInteger == firstRow['current_installed_ver']:
        return
      raise Exception('Not Implemented - update datastore from x to objectStoreHardCodedVersionInteger')
    self.executeInsideTransaction(someFn)


  def _resetDataForTest(self):
    def someFn(connectionContext):
      query = self.objDataTable.delete()
      connectionContext._INT_execute(query)
    self.executeInsideTransaction(someFn)

  def _getConnectionContext(self):
    return ConnectionContext(self)
