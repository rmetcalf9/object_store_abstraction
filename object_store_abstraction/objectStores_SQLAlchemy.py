from .objectStores_base import ObjectStore, ObjectStoreConnectionContext, StoringNoneObjectAfterUpdateOperationException, WrongObjectVersionException, ObjectStoreConfigError, MissingTransactionContextException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException, SuppliedObjectVersionWhenCreatingException
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, DateTime, JSON, \
  func, UniqueConstraint, and_, Text, select, update, delete
import pytz
##import datetime
from dateutil.parser import parse
import os
import logging
from .paginatedResult import getPaginatedResult
from .DoubleStringIndex import ConnectionContext_SQLAlchemy

from .makeDictJSONSerializable import getJSONtoPutInStore, getObjFromJSONThatWasPutInStore
from .paginatedResultIterator import PaginatedResultIteratorBaseClass, sortListOfKeysToDictBySortString, PaginatedResultIteratorFromDictWithAttrubtesAsKeysClass

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

objectStoreHardCodedVersionInteger = 2

class ConnectionContext(ObjectStoreConnectionContext):
  connection = None
  isTransactionInProgress = None
  objectStore = None

  def __init__(self, objectStore):
    super(ConnectionContext, self).__init__(doubleStringIndex=ConnectionContext_SQLAlchemy(main_context=self))
    self.objectStore = objectStore
    self.connection = self.objectStore.engine.connect()
    self.isTransactionInProgress = False

  def _startTransaction(self):
    if self.isTransactionInProgress:
      raise Exception("ERROR Starting transaction when there is already one in progress")
    ## _ = self.connection.begin() New auto begin behaviour
    self.isTransactionInProgress = True

  #Internal function for executing a statement
  ## only called from this file
  def _INT_execute(self, statement):
    if not self.isTransactionInProgress:
      MissingTransactionContextException
    return self.connection.execute(statement.execution_options(autocommit=False))

  def _commitTransaction(self):
    res = self.connection.commit()
    self.isTransactionInProgress = False
    return res

  def _rollbackTransaction(self):
    res = self.connection.rollback()
    self.isTransactionInProgress = False
    return res

  def _saveJSONObjectV2(self, objectType, objectKey, JSONString, objectVersion):
    #print("JSONString:", JSONString)
    query = (
      select(self.objectStore.objDataTable)  # 1. Start the select (pass the table/columns)
      .where(  # 2. Use the .where() method
        and_(
          self.objectStore.objDataTable.c.type == objectType,
          self.objectStore.objDataTable.c.key == objectKey
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
      return (newObjectVersion, curTime, curTime)
    if objectVersion is None:
      raise TryingToCreateExistingObjectException
    if str(firstRow.objectVersion) != str(objectVersion):
      raise WrongObjectVersionException
    newObjectVersion = firstRow.objectVersion + 1
    table = self.objectStore.objDataTable
    type_column = table.c.type
    key_column = table.c.key
    query = (
      update(table)
      .where(
        and_(
          type_column == objectType,
          key_column == objectKey
        )
      ).values(
        objectVersion=newObjectVersion,
        objectDICT=getJSONtoPutInStore(JSONString),
        lastUpdateDate=curTime,
        lastUpdateDate_iso8601=curTime.isoformat()
      )
    )
    result = self._INT_execute(query)
    if result.rowcount != 1:
      #print('Result count is ', result.rowcount)
      raise Exception('_saveJSONObject wrong number of rows updated')

    #not returning creation date time as it is not present without an extra query
    return (newObjectVersion, None, curTime)

  def _removeJSONObject(self, objectType, objectKey, objectVersion, ignoreMissingObject):
    table = self.objectStore.objDataTable
    type_column = table.c.type
    key_column = table.c.key

    query = (
      delete(table)  # 1. Use the standalone delete() function
      .where(  # 2. Use the .where() method instead of 'whereclause'
        and_(
          type_column == objectType,
          key_column == objectKey
        )
      )
    )
    result = self._INT_execute(query)
    if result.rowcount == 0:
      if not ignoreMissingObject:
        raise TriedToDeleteMissingObjectException

  def _INT_getTupleFromRow(self, row):
    dt = parse(row.creationDate_iso8601)
    creationDate = dt.astimezone(pytz.utc)
    dt = parse(row.lastUpdateDate_iso8601)
    lastUpdateDate = dt.astimezone(pytz.utc)
    convertedObjectDICT = getObjFromJSONThatWasPutInStore(row.objectDICT)

    return convertedObjectDICT, row.objectVersion, creationDate, lastUpdateDate, row.key


  #Return value is objectDICT, ObjectVersion, creationDate, lastUpdateDate
  #Return None, None, None, None if object isn't in store
  ObjTableKeyMap = None
  def _getObjectJSON(self, objectType, objectKey):
    table = self.objectStore.objDataTable
    type_column = table.c.type
    key_column = table.c.key

    query = (
      select(table)
      .where(
        and_(
          type_column == objectType,
          key_column == objectKey
        )
      )
    )
    result = self._INT_execute(query)
    firstRow = result.fetchone()
    if firstRow is None:
      return None, None, None, None, None
    if result.rowcount != 1:
      raise Exception('_getObjectJSON Wrong number of rows returned for key')

    return self._INT_getTupleFromRow(firstRow)

  def _list_all_objectTypes(self):
    results = []

    table = self.objectStore.objDataTable
    type_column = table.c.type

    # Refactored 2.0 statement:
    query = (
      select(type_column)  # 1. Pass the column(s) directly to select()
      .group_by(type_column)  # 2. Use the chainable .group_by() method
    )
    #print("QUERY:", query)
    result = self._INT_execute(query)

    fetching = True
    while (fetching):
      row = result.fetchone()
      if row is None:
        fetching = False
      else:
        results.append(row[0])

    return results

  def __getObjectTypeListFromDBUsingQuery(self, objectType, queryString, offset, pagesize):
    # TODO refactor to use iterator
    iterator = Iterator(queryString, None, None, self, objectType)
    srcData = {}
    curItem=iterator.next()
    while curItem is not None:
      srcData[curItem[4]] = curItem
      curItem=iterator.next()
    return srcData

  def _getPaginatedResult(self, objectType, paginatedParamValues, outputFN):
    srcData = {}
    if paginatedParamValues['sort'] is None:
      srcData = self.__getObjectTypeListFromDBUsingQuery(
        objectType,
        paginatedParamValues['query'],
        paginatedParamValues['offset'],
        paginatedParamValues['pagesize']
      )
    else:
      # Sort requires inspection of data. We must retrieve all data for it to work
      # this requires a full load of data if sorting is required otherwise some rows
      # won't be in order. This is one reason to move away from generic objectStore
      # library
      srcData = self.__getObjectTypeListFromDBUsingQuery(
        objectType,
        paginatedParamValues['query'],
        None,
        None
      )

    return getPaginatedResult(
      list=srcData,
      outputFN=outputFN,
      offset=paginatedParamValues['offset'],
      pagesize=paginatedParamValues['pagesize'],
      query=paginatedParamValues['query'],
      sort=paginatedParamValues['sort'],
      filterFN=self.filterFN_allowAll
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

  def _getPaginatedResultIterator(self, query, sort, filterFN, getSortKeyValueFn, objectType):
    iterator = Iterator(query, filterFN, getSortKeyValueFn, self, objectType)
    if sort is None:
      return iterator
    srcData = {}
    curItem=iterator.next()
    while curItem is not None:
      srcData[curItem[4]] = curItem
      curItem=iterator.next()

    # All filtering is already done, this iterator just sorts
    dict, query, sort, filterFN, getSortKeyValueFn
    return PaginatedResultIteratorFromDictWithAttrubtesAsKeysClass(
      dict=srcData,
      query=None,
      sort=sort,
      filterFN=None,
      getSortKeyValueFn=getSortKeyValueFn
    )

  def _truncateObjectType(self, objectType):
    table = self.objectStore.objDataTable
    type_column = table.c.type

    query = (
      delete(table)  # 1. Use the standalone delete() function
      .where(  # 2. Use the .where() method
        type_column == objectType
      )
    )
    result = self._INT_execute(query)

class ObjectStore_SQLAlchemy(ObjectStore):
  engine = None
  objDataTable = None
  idxDataTable = None
  verTable = None
  objectPrefix = None
  def __init__(self, ConfigDict, externalFns, detailLogging, type, factoryFn):
    super(ObjectStore_SQLAlchemy, self).__init__(externalFns, detailLogging, type)

    if detailLogging:
      logging.basicConfig()
      logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    if "connectionString" not in ConfigDict:
      raise ObjectStoreConfigError("APIAPP_OBJECTSTORECONFIG SQLAlchemy ERROR - Expected connectionString")
    if "objectPrefix" in ConfigDict:
      self.objectPrefix = ConfigDict["objectPrefix"]
    else:
      self.objectPrefix = ""

    otherArgsForCreateEngine = {}
    if "create_engine_args" in ConfigDict:
      otherArgsForCreateEngine = ConfigDict["create_engine_args"]
    else:
      otherArgsForCreateEngine = {
        "pool_recycle": 3600,
        "pool_size": 40,
        "max_overflow": 0
      }
      if "connect_args" in ConfigDict:
        if ConfigDict["connect_args"] is not None:
          otherArgsForCreateEngine["connect_args"] = ConfigDict["connect_args"]

    if "ssl_ca" in ConfigDict:
      #print("ssl_ca:", ConfigDict['ssl_ca'])
      if not os.path.isfile(ConfigDict['ssl_ca']):
        raise Exception("Supplied ssl_ca dosen't exist")
      if "connect_args" not in ConfigDict:
        ConfigDict["connect_args"] = {}

      ConfigDict["connect_args"]["ssl"] = {'ca': ConfigDict['ssl_ca']}

    if "poolclass" in otherArgsForCreateEngine:
      if otherArgsForCreateEngine["poolclass"] == "StaticPool":
        otherArgsForCreateEngine["poolclass"] = StaticPool

    if detailLogging:
      print("Creating connection with args:", otherArgsForCreateEngine)
    self.engine = create_engine(ConfigDict["connectionString"], **otherArgsForCreateEngine)

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
    self.idxDataTable = Table(self.objectPrefix + '_idxData', metadata,
        #Tired intorudcing a seperate primary key and using key column as index but
        # I found the same lenght restriction exists on an index
        Column('id', Integer, primary_key=True),
        Column('type', String(50), index=True),
        Column('keyA', String(140), index=True), #MariaDB has smaller limit on inexes
        Column('keyB', String(140), index=True),
        UniqueConstraint('type', 'keyA', name=self.objectPrefix + '_idxData_ix1'),
        UniqueConstraint('type', 'keyB', name=self.objectPrefix + '_idxData_ix2')
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

  def get_current_db_version(self, connectionContext, externalFns):
    curTime = externalFns['getCurDateTime']()
    query = self.verTable.select()
    result = connectionContext._INT_execute(query)
    if result.rowcount != 1:
      if result.rowcount != 0:
        raise Exception('invalid database structure - can\'t read version')
      # There are 0 rows, create one
      query = self.verTable.insert().values(
        first_installed_ver=objectStoreHardCodedVersionInteger,
        current_installed_ver=objectStoreHardCodedVersionInteger,
        creationDate_iso8601=curTime.isoformat(),
        lastUpdateDate_iso8601=curTime.isoformat()
      )
      result = connectionContext._INT_execute(query)
      return objectStoreHardCodedVersionInteger
    firstRow = result.mappings().first()
    return firstRow['current_installed_ver']

  def set_current_db_version(self, connectionContext, externalFns, newversion):
    curTime = externalFns['getCurDateTime']()
    query = self.verTable.update().values(
      current_installed_ver=newversion,
      lastUpdateDate_iso8601=curTime.isoformat()
    )
    result = connectionContext._INT_execute(query)
    return newversion

  def _INT_setupOrUpdateVer(self, externalFns):
    # Bring the database up to required version
    def someFn(connectionContext):
      return self.get_current_db_version(connectionContext, externalFns)

    current_db_version = self.executeInsideTransaction(someFn)
    if current_db_version == 1:
      print("Detected DB version 1 - updating to 2 - no changes required")
      def someFn2(connectionContext):
        return self.set_current_db_version(connectionContext, externalFns, 2)
      current_db_version = self.executeInsideTransaction(someFn2)


    if current_db_version < objectStoreHardCodedVersionInteger:
      print("Current Version", current_db_version)
      print("Required Version", objectStoreHardCodedVersionInteger)
      raise Exception('Not Implemented - update datastore from x to objectStoreHardCodedVersionInteger')


  def _resetDataForTest(self):
    def someFn(connectionContext):
      query = self.objDataTable.delete()
      connectionContext._INT_execute(query)
      query2 = self.idxDataTable.delete()
      connectionContext._INT_execute(query2)
    self.executeInsideTransaction(someFn)

  def _getConnectionContext(self):
    return ConnectionContext(self)

class Iterator(PaginatedResultIteratorBaseClass):
  sqlAlchemyStoreConnectionContext = None
  objectType = None
  result = None

  def __init__(self, query, filterFN, getSortKeyValueFn, sqlAlchemyStoreConnectionContext, objectType):
    if query is not None:
      # This mode deals with queries without a filter function
      if filterFN is None:
        def defFilterFn(item, whereClause):
          return True
        filterFN = defFilterFn
    PaginatedResultIteratorBaseClass.__init__(self, query, filterFN)
    self.sqlAlchemyStoreConnectionContext = sqlAlchemyStoreConnectionContext
    self.objectType = objectType

    self.sqlAlchemyStoreConnectionContext.objectStore.detailLog('__getObjectTypeListFromDBUsingQuery')
    self.sqlAlchemyStoreConnectionContext.objectStore.detailLog('   objectType:' + str(self.objectType))
    self.sqlAlchemyStoreConnectionContext.objectStore.detailLog('  query:' + str(query))
    whereclauseToUse = self.sqlAlchemyStoreConnectionContext.objectStore.objDataTable.c.type==self.objectType
    if query is not None:
      if query != '':
        self.sqlAlchemyStoreConnectionContext.objectStore.detailLog('   **Adding where clause**')
        whereclauseToUse = and_(
          whereclauseToUse,
          self.sqlAlchemyStoreConnectionContext.objectStore.objDataTable.c.objectDICT.ilike('%' + query + '%')
        )

    table = self.sqlAlchemyStoreConnectionContext.objectStore.objDataTable
    key_column = table.c.key

    queryObj = (
      select(table)
      .where(whereclauseToUse)  # Use the .where() method
      .order_by(key_column)  # Use the .order_by() method
    )
    # print("SQLAlch Itr queryObj", queryObj)
    self.result =  self.sqlAlchemyStoreConnectionContext._INT_execute(queryObj)

  def _next(self):
    row = self.result.fetchone()
    if row is None:
      return None
    return self.sqlAlchemyStoreConnectionContext._INT_getTupleFromRow(row)

'''

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
  '''
