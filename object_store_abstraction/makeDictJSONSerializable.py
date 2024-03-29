#Some types of object are not JSON serialbizle
# this class swaps them for RJM objects which are
import json
import decimal

standardTypeTagString = "rj5mt3ypebdf3ase_TYPE"

def getRJMJSONSerializableDICT(normalDICT):
  return _recursiveConvertFromNormalToRJM(normalDICT)

def getNormalDICTFromRJMJSONSerializableDICT(RJMDICT):
  return _recursiveConvertFromRJMToNormal(RJMDICT)

def getJSONtoPutInStore(origObj):
  #print("Putting in:", type(origObj), ":", origObj)
  convertedDict = getRJMJSONSerializableDICT(origObj)
  #print("Converted:", convertedDict)
  #print("ORig:", origObj)
  return json.dumps(convertedDict)

def getObjFromJSONThatWasPutInStore(jsonFromStore):
  jsonDict = json.loads(jsonFromStore)
  normalDict = getNormalDICTFromRJMJSONSerializableDICT(jsonDict)
  #print("Getting out:", type(normalDict), ":", jsonFromStore)
  return normalDict

def _recursiveConvertFromNormalToRJM(anyObj):
  if isinstance(anyObj,dict):
    output = {}
    for x in anyObj:
      output[x] = _recursiveConvertFromNormalToRJM(anyObj[x])
    return output
  if isinstance(anyObj,list):
    output = []
    for i in range(len(anyObj)):
      output.append(_recursiveConvertFromNormalToRJM(anyObj[i]))
    return output
  if isinstance(anyObj,bytes):
    return getRJMTypeFromOrigObj(anyObj).toJSONableDICT()
  if isinstance(anyObj,decimal.Decimal):
    return getRJMTypeFromOrigObj(anyObj).toJSONableDICT()

  #It was not a type that we recognise so return original
  return anyObj

def _recursiveConvertFromRJMToNormal(anyObj):
  if isinstance(anyObj,dict):
    if standardTypeTagString in anyObj:
      return getRJMTypeFromRJMTypeJSONDict(anyObj).toOrig()
    for x in anyObj:
      anyObj[x] = _recursiveConvertFromRJMToNormal(anyObj[x])
    return anyObj
  if isinstance(anyObj,list):
    for i in range(len(anyObj)):
      anyObj[i] = _recursiveConvertFromRJMToNormal(anyObj[i])
  return anyObj


def getRJMTypeFromOrigObj(obj):
  if isinstance(obj,bytes):
    return RJMTypeBytesClass(obj, None)
  if isinstance(obj,decimal.Decimal):
    return RJMTypeDecimalClass(obj, None)
  raise Exception("Unhandled specialType")

def getRJMTypeFromRJMTypeJSONDict(dictObjThatIsJSONRepresenationOfRJMTypeClass):
  if dictObjThatIsJSONRepresenationOfRJMTypeClass[standardTypeTagString] == 'RJMTypeBytesClass':
    return RJMTypeBytesClass(None, dictObjThatIsJSONRepresenationOfRJMTypeClass)
  if dictObjThatIsJSONRepresenationOfRJMTypeClass[standardTypeTagString] == 'RJMTypeDecimalClass':
    return RJMTypeDecimalClass(None, dictObjThatIsJSONRepresenationOfRJMTypeClass)
  raise Exception("Unknown type " + dictObjThatIsJSONRepresenationOfRJMTypeClass[standardTypeTagString])

class RJMTypeBaseClass:
  obj = None
  def __init__(self, obj, rjmTypeDict):
    if obj is None:
      self.obj = rjmTypeDict['data'].encode("utf-8")
      return
    self.obj = obj
  def toOrig(self):
    return self.obj

class RJMTypeBytesClass(RJMTypeBaseClass):
  def toJSONableDICT(self):
    return {
      standardTypeTagString: 'RJMTypeBytesClass',
      'data': self.obj.decode("utf-8")
    }

class RJMTypeDecimalClass(RJMTypeBaseClass):
  def toJSONableDICT(self):
    return {
      standardTypeTagString: 'RJMTypeDecimalClass',
      'data': str(self.obj)
    }
  def toOrig(self):
    return decimal.Decimal(self.obj.decode("utf-8"))
