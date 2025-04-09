from ObjectStoresWithPrefix import objectStoresWithPrefix, JSONString, JSONString2
import TestHelperSuperClass

'''
These tests require a local mysql database
 - DB IS RJM Personal infrastructure - password for DB is in docker-compose
To setup log into db as admin and run the following commands:

create database saas_user_man CHARACTER SET utf8 COLLATE utf8_general_ci;
create database saas_user_man_rad CHARACTER SET utf8 COLLATE utf8_general_ci;

## Check it is utf-8
SELECT schema_name, default_character_set_name FROM information_schema.SCHEMATA;
grant ALL on saas_user_man.* TO saas_user_man_user@'%' IDENTIFIED BY 'saas_user_man_testing_password';
grant ALL on saas_user_man_rad.* TO saas_user_man_user@'%';
FLUSH PRIVILEGES;
select host, user, ssl_type from user;

Alternative user creation syntax is:
CREATE USER 'saas_user_man_user'@'%' IDENTIFIED BY 'saas_user_man_testing_password';
grant ALL on saas_user_man.* TO saas_user_man_user@'%';
grant ALL on saas_user_man_rad.* TO saas_user_man_user@'%';

'''
import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests
import test_objectStores_GenericTests_DoubleStringIndex as genericTestsDoubleStringIndex

from test_objectStores_GenericTests import addSampleRows, assertCorrectPaginationResult

import object_store_abstraction as undertest

import os
SKIPSQLALCHEMYTESTS=False
if ('SKIPSQLALCHEMYTESTS' in os.environ):
  if os.environ["SKIPSQLALCHEMYTESTS"]=="Y":
    SKIPSQLALCHEMYTESTS=True



SQLAlchemy_LocalDBConfigDict = {
  "Type":"SQLAlchemy",
  "connectionString":"mysql+pymysql://saas_user_man_user:saas_user_man_testing_password@127.0.0.1:10103/saas_user_man"
}
SQLAlchemy_LocalDBConfigDict_withPrefix = copy.deepcopy(SQLAlchemy_LocalDBConfigDict)
SQLAlchemy_LocalDBConfigDict_withPrefix["objectPrefix"] ="testPrefix"


class dummyException(Exception):
  pass

#@TestHelperSuperClass.wipd
class test_objectStoresSQLAlchemy(objectStoresWithPrefix):
  def test_genericTests(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return
    def getObjFn(SQLAlchemy_LocalDBConfigDict, resetData = True):
      obj = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      return obj
    genericTests.runAllGenericTests(self, getObjFn, SQLAlchemy_LocalDBConfigDict)

  def test_genericTests_doublestringindex(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return
    def getObjFn(SQLAlchemy_LocalDBConfigDict, resetData = True):
      obj = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      return obj
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, SQLAlchemy_LocalDBConfigDict)


  #Different prefixes don't share data
  def test_differentPrefixesDontShareData(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return
    obj = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    obj.resetDataForTest()
    obj2 = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict_withPrefix, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    obj2.resetDataForTest()
    self.differentPrefixesDontShareData(self, obj, obj2)

  #Test rollback single transaction
  def test_rollbackTransactionIsSuccessful_InsertOnly(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return
    obj = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    obj.resetDataForTest()

    def dbfn(storeConnection):
      #Test creation of record rollback works
      # _no data to start with
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = storeConnection.getObjectJSON("Test", "1_123")
      self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='Object found before it was added')

      # insert data
      def someFn(connectionContext):
        connectionContext.saveJSONObject("Test", "1_123", JSONString, None)
        (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = connectionContext.getObjectJSON("Test", "1_123")
        self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='object never added')
        raise dummyException("rollback")
      try:
        storeConnection.executeInsideTransaction(someFn)
      except dummyException:
        pass

      # _no data after rolledback insert start with
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = storeConnection.getObjectJSON("Test", "1_123")
      self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='Found but it should have rolled back')

    obj.executeInsideConnectionContext(dbfn)
  def test_rollbackTransactionIsSuccessful_UpdateOnly(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return
    obj = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    obj.resetDataForTest()

    def dbfn(storeConnection):
      # insert data
      def someFn(connectionContext):
        objVer = connectionContext.saveJSONObject("Test", "1_123", JSONString, None)
        (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = connectionContext.getObjectJSON("Test", "1_123")
        self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='object not added')
        return objVer
      objVer = storeConnection.executeInsideTransaction(someFn)

      # _no data to start with
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = storeConnection.getObjectJSON("Test", "1_123")
      self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='Object found before it was added')

      # update data
      def someFn(connectionContext):
        connectionContext.saveJSONObject("Test", "1_123", JSONString2, objVer)
        (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = connectionContext.getObjectJSON("Test", "1_123")
        self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString2, [  ], msg='object not updated')
        raise dummyException("rollback")
      try:
        storeConnection.executeInsideTransaction(someFn)
      except dummyException:
        pass

      # Make sure data has revereted to origional value
      (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objKey) = storeConnection.getObjectJSON("Test", "1_123")
      self.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='Did not roll back to previous value')

    obj.executeInsideConnectionContext(dbfn)

  def test_filter(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return

    objectStoreType = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    objectStoreType.resetDataForTest()
    testClass = self


    def dbfn(storeConnection):
      addSampleRows(storeConnection, 5, 'yyYYyyy')
      addSampleRows(storeConnection, 5, 'xxxxxxx', 5)
      def outputFN(item):
        return item[0]
      paginatedParamValues = {
        'offset': 0,
        'pagesize': 10,
        'query': 'dfgdbdfgfgfvfdgfd',
        'sort': None
      }
      res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
      expectedRes = []
      assertCorrectPaginationResult(testClass, res, 0, 10, 0)
      self.assertJSONStringsEqualWithIgnoredKeys(res['result'], expectedRes, [  ], msg='Wrong result')
    objectStoreType.executeInsideConnectionContext(dbfn)

  def test_detailLogging(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return

    objectStoreType = undertest.ObjectStore_SQLAlchemy(SQLAlchemy_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    objectStoreType.resetDataForTest()
    testClass = self


    def dbfn(storeConnection):
      addSampleRows(storeConnection, 5, 'yyYYyyy')
      addSampleRows(storeConnection, 5, 'xxxxxxx', 5)
      def outputFN(item):
        return item[0]
      paginatedParamValues = {
        'offset': 0,
        'pagesize': 10,
        'query': 'dfgdbdfgfgfvfdgfd',
        'sort': None
      }
      res = storeConnection.getPaginatedResult("Test1", paginatedParamValues, outputFN)
      expectedRes = []
      assertCorrectPaginationResult(testClass, res, 0, 10, 0)
      self.assertJSONStringsEqualWithIgnoredKeys(res['result'], expectedRes, [  ], msg='Wrong result')
    objectStoreType.executeInsideConnectionContext(dbfn)
