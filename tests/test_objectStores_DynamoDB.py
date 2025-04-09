from ObjectStoresWithPrefix import objectStoresWithPrefix
import TestHelperSuperClass

import copy
import datetime
import pytz

import test_objectStores_GenericTests as genericTests
from test_objectStores_GenericTests import addSampleRows, assertCorrectPaginationResult
import test_objectStores_GenericTests_DoubleStringIndex as genericTestsDoubleStringIndex

import object_store_abstraction as undertest

#Skipping dynmo tests using same rule as SQLAlchemy
import os
SKIPSQLALCHEMYTESTS=False
if ('SKIPSQLALCHEMYTESTS' in os.environ):
  if os.environ["SKIPSQLALCHEMYTESTS"]=="Y":
    SKIPSQLALCHEMYTESTS=True

DynamoDB_LocalDBConfigDict = {
  "Type": "DynamoDB",
  "aws_access_key_id": "ACCESS_KEY",
  "aws_secret_access_key": "SECRET_KEY",
  "region_name": "eu-west-2",
  "endpoint_url": "http://localhost:10111",
  "single_table_mode": "true"
}
DynamoDB_LocalDBConfigDict_withPrefix = copy.deepcopy(DynamoDB_LocalDBConfigDict)
DynamoDB_LocalDBConfigDict_withPrefix["objectPrefix"] ="testPrefix"


DynamoDB_LocalDBConfigDict_multiTable = {
  "Type": "DynamoDB",
  "aws_access_key_id": "ACCESS_KEY",
  "aws_secret_access_key": "SECRET_KEY",
  "region_name": "eu-west-2",
  "endpoint_url": "http://localhost:10111",
  "single_table_mode": "false"
}

class test_objectStoresDynamoDB(objectStoresWithPrefix):
  def test_genericTests(self):
    if SKIPSQLALCHEMYTESTS:
      print("SKIPSQLALCHEMYTESTS Set Skipping DynamoDB")
      return
    def getObjFn(DynamoDB_LocalDBConfigDict, resetData = True):
      obj = undertest.ObjectStore_DynamoDB(DynamoDB_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      return obj
    genericTests.runAllGenericTests(self, getObjFn, DynamoDB_LocalDBConfigDict)

  def test_genericTests_MultiTable(self):
    if SKIPSQLALCHEMYTESTS:
      print("SKIPSQLALCHEMYTESTS Set Skipping DynamoDB")
      return
    def getObjFn(DynamoDB_LocalDBConfigDict, resetData = True):
      obj = undertest.ObjectStore_DynamoDB(DynamoDB_LocalDBConfigDict_multiTable, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      return obj
    genericTests.runAllGenericTests(self, getObjFn, DynamoDB_LocalDBConfigDict_multiTable)

  def test_genericTests_doublestringindex(self):
    if SKIPSQLALCHEMYTESTS:
      print("SKIPSQLALCHEMYTESTS Set Skipping DynamoDB")
      return
    def getObjFn(DynamoDB_LocalDBConfigDict, resetData = True):
      obj = undertest.ObjectStore_DynamoDB(DynamoDB_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      return obj
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, DynamoDB_LocalDBConfigDict)

  def test_genericTests_doublestringindex_MultiTable(self):
    if SKIPSQLALCHEMYTESTS:
      print("SKIPSQLALCHEMYTESTS Set Skipping DynamoDB")
      return
    def getObjFn(DynamoDB_LocalDBConfigDict, resetData = True):
      obj = undertest.ObjectStore_DynamoDB(DynamoDB_LocalDBConfigDict_multiTable, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
      if resetData:
        obj.resetDataForTest()
      return obj
    genericTestsDoubleStringIndex.runAllGenericTests(self, getObjFn, DynamoDB_LocalDBConfigDict_multiTable)

  #Different prefixes don't share data
  def test_differentPrefixesDontShareData(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return
    obj = undertest.ObjectStore_DynamoDB(DynamoDB_LocalDBConfigDict, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    obj.resetDataForTest()
    obj2 = undertest.ObjectStore_DynamoDB(DynamoDB_LocalDBConfigDict_withPrefix, self.getObjectStoreExternalFns(), detailLogging=False, type='testSQLA', factoryFn=undertest.createObjectStoreInstance)
    obj2.resetDataForTest()
    self.differentPrefixesDontShareData(self, obj, obj2)


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
