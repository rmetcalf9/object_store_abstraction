import TestHelperSuperClass
import datetime
import pytz
import object_store_abstraction as undertest

class helpers():
  pass

class test_ductStructureChecks(TestHelperSuperClass.testHelperSuperClass):
  def test_cannotsaveWithNestedDateTime(self):
    objectType="testObjectType"
    objectKey="someKey"
    curDateTime = datetime.datetime.now(pytz.timezone("UTC"))
    dictToSave = {
      "A": {
        "B": {
          "C": curDateTime
        }
      }
    }
    ConfigDictI = {
      "Type": "Memory"
    }
    storeConnection = undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      return connectionContext.saveJSONObjectV2(
        objectType=objectType,
        objectKey=objectKey,
        JSONString=dictToSave,
        objectVersion = None
      )

    with self.assertRaises(Exception) as context:
      storeConnection.executeInsideTransaction(someFn)
    self.checkGotRightExceptionType(context,undertest.SavingDateTimeTypeException)

  def test_cansaveWithNestedNone(self):
    objectType="testObjectType"
    objectKey="someKey"
    curDateTime = datetime.datetime.now(pytz.timezone("UTC"))
    dictToSave = {
      "A": {
        "B": {
          "C": None
        }
      }
    }
    ConfigDictI = {
      "Type": "Memory"
    }
    storeConnection = undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      return connectionContext.saveJSONObjectV2(
        objectType=objectType,
        objectKey=objectKey,
        JSONString=dictToSave,
        objectVersion = None
      )

    storeConnection.executeInsideTransaction(someFn)

  def test_cannotsaveWithDateTimeInsideAList(self):
    objectType="testObjectType"
    objectKey="someKey"
    curDateTime = datetime.datetime.now(pytz.timezone("UTC"))
    dictToSave = {
      "A": {
        "B": {
          "C": [ curDateTime, curDateTime, curDateTime ]
        }
      }
    }
    ConfigDictI = {
      "Type": "Memory"
    }
    storeConnection = undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      return connectionContext.saveJSONObjectV2(
        objectType=objectType,
        objectKey=objectKey,
        JSONString=dictToSave,
        objectVersion = None
      )

    with self.assertRaises(Exception) as context:
      storeConnection.executeInsideTransaction(someFn)
    self.checkGotRightExceptionType(context,undertest.SavingDateTimeTypeException)

  def test_cannotsaveWithDateTimeInsideADictInAListList(self):
    objectType="testObjectType"
    objectKey="someKey"
    curDateTime = datetime.datetime.now(pytz.timezone("UTC"))
    badDict = {
      "e": 12,
      "g": {
        "h": curDateTime
      }
    }
    dictToSave = {
      "A": {
        "B": {
          "C": [ 123, badDict, "ff" ]
        }
      }
    }
    ConfigDictI = {
      "Type": "Memory"
    }
    storeConnection = undertest.createObjectStoreInstance(ConfigDictI, self.getObjectStoreExternalFns())

    def someFn(connectionContext):
      #print(self.jobs[jobGUID]._caculatedDict(self.appObj))
      return connectionContext.saveJSONObjectV2(
        objectType=objectType,
        objectKey=objectKey,
        JSONString=dictToSave,
        objectVersion = None
      )

    with self.assertRaises(Exception) as context:
      storeConnection.executeInsideTransaction(someFn)
    self.checkGotRightExceptionType(context,undertest.SavingDateTimeTypeException)
