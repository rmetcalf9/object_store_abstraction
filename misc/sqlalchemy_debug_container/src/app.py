from baseapp_for_restapi_backend_with_swagger import readFromEnviroment
import datetime
import pytz
import os
import json
from object_store_abstraction import createObjectStoreInstance




print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("Start of main.py")

objectStoreConfigJSON = readFromEnviroment(os.environ, 'APIAPP_OBJECTSTORECONFIG', '{}', None)
objectStoreConfigDict = None
try:
  if objectStoreConfigJSON != '{}':
    objectStoreConfigDict = json.loads(objectStoreConfigJSON)
except Exception as err:
  raise err

def getCurDateTime():
  return datetime.datetime.now(pytz.timezone("UTC"))

fns = {
  'getCurDateTime': getCurDateTime
}
objectStoreInstance = createObjectStoreInstance(objectStoreConfigDict, fns)

print("objectStoreInstance:", objectStoreInstance)

def someFn(connectionContext):
  paginatedParamValues = {}
  paginatedParamValues['query'] = ""
  paginatedParamValues['sort'] = ""
  paginatedParamValues['offset'] = 0
  paginatedParamValues['pagesize'] = 100

  a = connectionContext.getPaginatedResult("users", paginatedParamValues, None)

  print(a)
objectStoreInstance.executeInsideConnectionContext(someFn)



print("End of main.py")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
