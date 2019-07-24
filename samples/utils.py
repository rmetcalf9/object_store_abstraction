import os
import json
import datetime
import pytz
from object_store_abstraction import createObjectStoreInstance, TryingToCreateExistingObjectExceptionClass

space = "                                 "


configs = []
for f in os.listdir("./datastore_configs"):
  if os.path.isfile("./datastore_configs/" + f):
    configs.append({
      "shortName": f,
      "full": "./datastore_configs/" + f
    })

datasets = []
for f in os.listdir("./sample_data"):
  if os.path.isfile("./sample_data/" + f):
    datasets.append({
      "shortName": f,
      "full": "./sample_data/" + f
    })


def getCurDateTime():
  return datetime.datetime.now(pytz.timezone("UTC"))

def userSelectOptionFromList(prompt, list, dispItem):
  if len(list)==0:
    raise Exception("No items to select from")
  if len(list)==1:
    return 0
  print(prompt)
  for n in range(len(list)):
    if dispItem == None:
      print(n, ":", list[n])
    else:
      print(n, ":", list[n][dispItem])

  inpL = input().strip().upper()
  if inpL == "Q":
    exit(0)
  num = int(inpL)

  valid = False
  if num > -1:
    if num < len(list):
      valid = True

  if not valid:
    raise Exception("Invalid selection")

  return num

def getAnObjectStoreFromUser(
  prompt, giveUserOptionToReturnTwoObjectStoresIfItIsMigrateType=False
):
  selectedConfig = userSelectOptionFromList(prompt, configs, "shortName")

  f=open(configs[selectedConfig]["full"], "r")
  objectStoreConfigDict =  json.loads(f.read())
  f.close()

  fns = {
    'getCurDateTime': getCurDateTime
  }


  if giveUserOptionToReturnTwoObjectStoresIfItIsMigrateType:
    if objectStoreConfigDict["Type"]=="Migrating":
      objectStoreFromDict = objectStoreConfigDict["From"]
      objectStoreToDict = objectStoreConfigDict["To"]
      print("Creating FROM object store...")
      objectStoreFrom = createObjectStoreInstance(
        objectStoreConfigDict["From"],
        fns,
        detailLogging=False
      )
      print("Creating TO object store...")
      objectStoreTo = createObjectStoreInstance(
        objectStoreConfigDict["To"],
        fns,
        detailLogging=False
      )
      return objectStoreFrom, objectStoreTo

  print("Creating object store...")

  objectStore = createObjectStoreInstance(
    objectStoreConfigDict,
    fns,
    detailLogging=False
  )
  return objectStore

def getMigrtionObjectStorePairFromUser():
  storeFrom = None
  x = getAnObjectStoreFromUser("Select Store you are migrating FROM", giveUserOptionToReturnTwoObjectStoresIfItIsMigrateType=True)
  if isinstance(x, tuple):
    storeFrom = x[0]
    storeTo = x[1]
  else:
    storeFrom = x
    storeTo = getAnObjectStoreFromUser("Select Store you are migrating TO")
  return storeFrom, storeTo

def getADataset(prompt):
  selectedDataset = userSelectOptionFromList(prompt, datasets, "shortName")
  data = []
  f=open(datasets[selectedDataset]["full"], "r")
  dateLines = f.readlines()
  f.close()
  for x in dateLines:
    data.append(json.loads(x))
  return data

#This function is now in library I will remove it from here when library is updated
def outputFnJustKeys(item):
  (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = item
  return objectKey
