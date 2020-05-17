
import unittest

import copy
import json
import decimal

import object_store_abstraction as undertest

import TestHelperSuperClass

JSONString = {
  'AA': "AA",
  'BB': "BB",
  "CC": {
    "CC.AA": "AA",
    "CC.BB": "BB",
    "CC.CC": "CC"
  },
  'exampleByteObject': b'abc',
  'exampleListObject': [1, 2, 3],
  'exampleListObjectWithSubObject': [{'a':'a', 'bytes': b'b'}, {'bytes': b'b'}],
  'listOfBytes': [b'abc1', b'abc2'],
  'bytesInsideDict': {'key1': 'val1', 'keyWithBytes':b'abc1'},
  'bytesInsideDictInsideDict': {'key1': 'val1', 'subDict': { 'subkey1': 'val1', 'subbyte':b'abc1'}},
  'bytesInsideDictInsideListOfDict': {'key1': 'val1', 'subDictList': [
    { 'subkey1': 'val1', 'subbyte':b'abc1'},
    { 'subkey1': 'val1', 'subbyte':b'abc1'},
    { 'subkey1': 'val1', 'subbyte':b'abc1'}
  ]}
}

JSONString2 = {
  "Name": "usersystem",
  "Description": "Master Tenant for User Management System",
  "AllowUserCreation": False,
  "AuthProviders": {
    "3662ab29-0594-42fc-bfc6-60f1a29dfa92": {
      "guid": "3662ab29-0594-42fc-bfc6-60f1a29dfa92",
      "MenuText": "Website account login",
      "IconLink": None,
      "Type": "internal",
      "AllowUserCreation": False,
      "ConfigJSON": {"userSufix": "@internalDataStore"},
      "saltForPasswordHashing": "JDJiDFFGLJFEFFJMmSADSDD....lNSFRqeTNlWVVBYk8="
    }
  }
}

testCaseList = []
testCaseList.append(('EmptyDICT', {}))
testCaseList.append(('SimpleJSONString', {'a': 'a', 'b': 'b'}))
testCaseList.append(('SimpleJSONInteger', {'a': 1, 'b': 2}))
testCaseList.append(('SimpleJSONMixed', {'a': '1', 'b': 2}))
testCaseList.append(('EmptyList', {'a': []}))
testCaseList.append(('ListString', {'a': ['a','b','c']}))
testCaseList.append(('ListInteger', {'a': [1,2,3]}))
#testCaseList.append(('ListMixed', {'a': [1,'2',3]})) mixed lists not supported due to my assert equal
testCaseList.append(('ListDict', {'a': [{'a':1,'b':'2'}]}))
testCaseList.append(('Byte', {'a': b'someBytes'}))
testCaseList.append(('ListByte', {'a': [b'someBytes1',b'someBytes2']}))
testCaseList.append(('ListObject', {'a': [{'a': 'a'},{'b': 'b'}]}))
testCaseList.append(('ListObjectBytes', {'a': [{'a': b'a'},{'b': b'b'}]}))
testCaseList.append(('ListObjectBytesMixed', {'a': [{'a': b'a'},{'b': b'b'},{'c': 'c'}]}))
testCaseList.append(('JSONString', JSONString))
testCaseList.append(('JSONString2', JSONString2))
testCaseList.append(('ObjectWithDecimalString',{"id": "9e8426b9-6a4a-467d-b667-b95f0b01b077", "x": 103.91526765470616, "y": -45.090213780814636}))
testCaseList.append(('DecimalObject', decimal.Decimal('5.5')))

#@TestHelperSuperClass.wipd
class testConversionToJSONSerailisable(unittest.TestCase):
  def test_allCasesGiveSameResult(self):
    for x in testCaseList:
      rjmVer = undertest.getRJMJSONSerializableDICT(copy.deepcopy(x[1]))
      processedVer = undertest.getNormalDICTFromRJMJSONSerializableDICT(rjmVer)

      self.assertEqual(processedVer, x[1], msg=x[0] + ' - didn\'t map back to orignial value')

  def test_allCasesGiveJSONSearilizableVersions(self):
    for x in testCaseList:
      rjmVer = undertest.getRJMJSONSerializableDICT(copy.deepcopy(x[1]))
      try:
        jsonString = json.dumps(rjmVer)
      except Exception as a:
        print("Test case " + x[0] + " failed:")
        raise
