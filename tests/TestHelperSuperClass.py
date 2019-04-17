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
from sortedcontainers import SortedDict

def getPaginatedResult(list, outputFN, request, filterFN):
  pagesizemax = 100
  offset = request.args.get('offset')
  if offset is None:
    offset = 0
  else:
    offset = int(offset)
  pagesize = request.args.get('pagesize')
  if pagesize is None:
    pagesize = 100
  else:
    pagesize = int(pagesize)

  # limit rows returned per request
  if pagesize > pagesizemax:
    pagesize = pagesizemax

  if request.args.get('query') is not None:
    origList = SortedDict(list)
    list = SortedDict()
    where_clauses = request.args.get('query').strip().upper().split(" ")
    def includeItem(item):
      for curClause in where_clauses:
        if not filterFN(item, curClause):
          return False
      return True
    for cur in origList:
      if includeItem(origList[cur]):
        list[cur]=origList[cur]

  # we now have "list" which is a filtered down list of things we need to return
  #construct a list of keys to the object, all null
  sortedKeys = []
  for cur in list:
    sortedKeys.append(cur)

  #Sort sortedKeys
  if request.args.get('sort') is not None:
    def getSortTuple(key):
      #sort keys are case sensitive
      kk = key.split(":")
      if len(kk)==0:
        raise Exception('Invalid sort key')
      elif len(kk)==1:
        return {'name': kk[0], 'desc': False}
      elif len(kk)==2:
        if kk[1].lower() == 'desc':
          return {'name': kk[0], 'desc': True}
        elif kk[1].lower() == 'asc':
          return {'name': kk[0], 'desc': False}
      raise Exception('Invalid sort key - ' + key)

    def genSortKeyGenFn(listBeingSorted, sortkey):
      def sortKeyGenFn(ite):
        try:
          # print(sortkey)
          # print(outputFN(listBeingSorted[ite])[sortkey])
          ret = outputFN(listBeingSorted[ite])[sortkey]
          if ret is None:
            return ''
          if isinstance(ret, int):
            return ('000000000000000000000000000000000000000000000000000' + str(ret))[-50:]
          if isinstance(ret, bool):
            if ret:
              return 'True'
            return 'False'
          return ret
        except KeyError:
          raise Exception('Sort key ' + sortkey + ' not found')
      return sortKeyGenFn

    # sort by every sort key one at a time starting with the least significant
    for curSortKey in request.args.get('sort').split(",")[::-1]:
      sk = getSortTuple(curSortKey)
      sortedKeys.sort(key=genSortKeyGenFn(list, sk['name']), reverse=sk['desc'])

  output = []
  for cur in range(offset, (pagesize + offset)):
    if (cur<len(list)):
      output.append(outputFN(list[sortedKeys[cur]]))
      #output.append(outputFN(list[list.keys()[sortedKeys[cur]]]))

  return {
    'pagination': {
      'offset': offset,
      'pagesize': pagesize,
      'total': len(list)
    },
    'result': output
  }


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
      'getCurDateTime': self.getCurDateTime,
      'getPaginatedResult': getPaginatedResult
    }    

  def checkGotRightException(self, context, ExpectedException):
    if (context.exception != None):
      if (context.exception != ExpectedException):
        print("**** Wrong exception raised:")
        print("      expected: " + type(ExpectedException).__name__ + ' - ' + str(ExpectedException));
        print("           got: " + type(context.exception).__name__ + ' - ' + str(context.exception));
        raise context.exception
    self.assertTrue(ExpectedException == context.exception)

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



