from baseapp_for_restapi_backend_with_swagger import readFromEnviroment
import datetime
import pytz
import os
import json
from object_store_abstraction import createObjectStoreInstance
import copy

import logging


print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("Start of main.py\n")

#Configure SQLAlchemy to log all statements
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

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
  #paginatedParamValues['query'] = ""
  #paginatedParamValues['sort'] = ""
  paginatedParamValues['query'] = None
  paginatedParamValues['sort'] = None
  paginatedParamValues['offset'] = 0
  paginatedParamValues['pagesize'] = 100

  print("---")
  print("-A call with NO query-")
  print("---")
  a = connectionContext.getPaginatedResult("users", copy.deepcopy(paginatedParamValues), None)
  print("Res:", a)

  paginatedParamValues['query'] = "exampleQueryString"
  print("---")
  print("-B call with query-")
  print("---")
  a = connectionContext.getPaginatedResult("users", copy.deepcopy(paginatedParamValues), None)
  print("Res:", a)

  paginatedParamValues['query'] = "codefresh"
  print("---")
  print("-C call with single return-")
  print("---")
  a = connectionContext.getPaginatedResult("users", copy.deepcopy(paginatedParamValues), None)
  print("Res:", a)

objectStoreInstance.executeInsideConnectionContext(someFn)



print("\nEnd of main.py")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
print("---------------------------------------")
