

import utils


def outputFnKeysAndData(item):
  (objectDICT, ObjectVersion, creationDate, lastUpdateDate, objectKey) = item
  #print("objectDICT:", objectDICT)
  #print("ObjectVersion:", ObjectVersion)
  #print("creationDate:", creationDate)
  #print("lastUpdateDate:", lastUpdateDate)
  #print("objectKey:", objectKey)
  return objectKey, objectDICT


def cmd():
  storeFrom, storeTo = utils.getMigrtionObjectStorePairFromUser()


  def dbfn2(storeConnectionFrom):
    def dbfn3(storeConnectionTo):
      totalMigratedObjects = 0
      totalAlreadyMigrated = 0

      objectTypesFrom = storeConnectionFrom.list_all_objectTypes()

      for objectType in objectTypesFrom:
        print("Migrating " + objectType)
        allKeys = storeConnectionFrom.getAllRowsForObjectType(
          objectType=objectType,
          filterFN=None,
          outputFN=outputFnKeysAndData,
          whereClauseText=None
        )
        for (objectKey, objectData) in allKeys:
          JSONString, _, _, _, _ = storeConnectionTo.getObjectJSON(objectType, objectKey)
          if JSONString == None:
            print("Migrating " + objectKey)
            savedVer = storeConnectionTo.saveJSONObject(objectType, objectKey, objectData, objectVersion = None)
            totalMigratedObjects = totalMigratedObjects + 1
          else:
            totalAlreadyMigrated = totalAlreadyMigrated + 1


      print("--------------")
      if totalMigratedObjects == 0:
        print("No data to migrate")
      else:
        print("Migrated " + str(totalMigratedObjects))
        print("already migrated " + str(totalAlreadyMigrated))

      return None

    return storeTo.executeInsideTransaction(dbfn3)
  retVal = storeFrom.executeInsideTransaction(dbfn2)

  print("\n")

def getCommand():
  return {
    'Name': 'Preform migration',
    'fn': cmd
  }
