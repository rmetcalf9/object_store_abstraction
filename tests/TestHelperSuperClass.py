#Test helper functions
# defines a baseclass with extra functions
# https://docs.python.org/3/library/unittest.html
import unittest
import json
import copy
import datetime
import pytz
import jwt
from base64 import b64decode

class testHelperSuperClass(unittest.TestCase):
  curDateTimeOverrideForTesting = None
  def __init__(self, a):
    self.curDateTimeOverrideForTesting = None
    super().__init__(a)

  def setTestingDateTime(self, val):
    self.curDateTimeOverrideForTesting = val
  def getCurDateTime(self):
    if self.curDateTimeOverrideForTesting is None:
      return datetime.datetime.now(pytz.timezone("UTC"))
    return self.curDateTimeOverrideForTesting
  def getObjectStoreExternalFns(self):
    return {
      'getCurDateTime': self.getCurDateTime
    }

  def checkGotRightException(self, context, ExpectedException):
    if (context.exception != None):
      if (context.exception != ExpectedException):
        print("**** Wrong exception raised:")
        print("      expected: " + type(ExpectedException).__name__ + ' - ' + str(ExpectedException));
        print("           got: " + type(context.exception).__name__ + ' - ' + str(context.exception));
        print("")
        if context.exception.__traceback__ is None:
          print("No traceback data in origional exception")
        else:
          print("Origional exception Traceback: ", context.exception.__traceback__)
        print("context", context)
        raise context.exception
    self.assertTrue(ExpectedException == context.exception)

  def checkGotRightExceptionType(self, context, ExpectedException, msg=""):
    if (context.exception != None):
      if (context.exception != ExpectedException):
        if (not isinstance(context.exception,ExpectedException)):
          print("**** Wrong exception TYPE raised:")
          print("      expected: " + type(ExpectedException).__name__ + ' - ' + str(ExpectedException));
          print("           got: " + type(context.exception).__name__ + ' - ' + str(context.exception));
          print("")
          if context.exception.__traceback__ is None:
            print("No traceback data in origional exception")
          else:
            print("Origional exception Traceback: ", context.exception.__traceback__)
          raise context.exception


  def sortAllMembers(self, objToSotr):
    if isinstance(objToSotr,list):
      for k in objToSotr:
        self.sortAllMembers(k)
      if len(objToSotr)>1:
        if isinstance(objToSotr[0],dict):
          return #list has dicts inside so no way of sorting
        objToSotr.sort()
      return
    if isinstance(objToSotr,dict):
      for k in objToSotr.keys():
        self.sortAllMembers(objToSotr[k])
      return

  def convertAnyByteValueToString(self, val):
    if isinstance(val,list):
      for a in val:
        self.convertAnyByteValueToString(a)
    if isinstance(val,dict):
      for a in val:
        if isinstance(val[a],bytes):
          #print("CHANGING TO UTF:", val[a])
          val[a] = val[a].decode("utf-8")
        self.convertAnyByteValueToString(val[a])
    else:
      pass

  def areJSONStringsEqual(self, str1, str2):
    self.sortAllMembers(str1)
    self.sortAllMembers(str2)
    self.convertAnyByteValueToString(str1)
    self.convertAnyByteValueToString(str2)
    a = json.dumps(str1, sort_keys=True)
    b = json.dumps(str2, sort_keys=True)
    return (a == b)

  def assertJSONStringsEqual(self, str1, str2, msg=''):
    if (self.areJSONStringsEqual(str1,str2)):
      return
    print("Mismatch JSON")
    a = json.dumps(str1, sort_keys=True)
    b = json.dumps(str2, sort_keys=True)
    print(a)
    print("--")
    print(b)
    self.assertTrue(False, msg=msg)

  #provide a list of ignored keys
  def assertJSONStringsEqualWithIgnoredKeys(self, str1, str2, ignoredKeys, msg=''):
    cleaned1 = copy.deepcopy(str1)
    cleaned2 = copy.deepcopy(str2)
    for key_to_ignore in ignoredKeys:
      keyPresentInEither = False
      if key_to_ignore in cleaned1:
        keyPresentInEither = True
      if key_to_ignore in cleaned2:
        keyPresentInEither = True
      if keyPresentInEither:
        cleaned1[key_to_ignore] = 'ignored'
        cleaned2[key_to_ignore] = 'ignored'
    return self.assertJSONStringsEqual(cleaned1, cleaned2, msg)

  def assertTimeCloseToCurrent(self, time, msg='Creation time is more than 3 seconds adrift'):
    if (isinstance(time, str)):
      time = from_iso8601(time)
    curTime = datetime.datetime.now(pytz.timezone("UTC"))
    time_diff = (curTime - time).total_seconds()
    self.assertTrue(time_diff < 3, msg=msg)

  def assertResponseCodeEqual(self, result, expectedResponse, msg=''):
    if result.status_code==expectedResponse:
      return
    print(result.get_data(as_text=True))
    self.assertEqual(result.status_code, expectedResponse, msg)

def getHashedPasswordUsingSameMethodAsJavascriptFrontendShouldUse(username, password, tenantAuthProvSalt):
  masterSecretKey = (username + ":" + password + ":AG44")
  ret = appObj.bcrypt.hashpw(masterSecretKey, b64decode(tenantAuthProvSalt))
  return ret
