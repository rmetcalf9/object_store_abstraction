

import utils


def cmd():
  storeFrom, storeTo = utils.getMigrtionObjectStorePairFromUser()

  def dbfn2(storeConnectionFrom):
    def dbfn3(storeConnectionTo):
      mperc = -1
      objectTypesFrom = storeConnectionFrom.list_all_objectTypes()
      objectTypesTo = storeConnectionFrom.list_all_objectTypes()

      totalObjects = 0
      totalMigratedObjects = 0

      for objectType in objectTypesFrom:
        allKeys = storeConnectionFrom.getAllRowsForObjectType(
          objectType=objectType,
          filterFN=None,
          outputFN=utils.outputFnJustKeys,
          whereClauseText=None
        )
        numObjects = len(allKeys)
        numMigrated = 0
        if objectType in objectTypesTo:
          for curKey in allKeys:
            obj, _, _, _, _ = storeConnectionTo.getObjectJSON(objectType, curKey)
            if obj != None:
              numMigrated = numMigrated + 1

        totalObjects = totalObjects + len(allKeys)
        totalMigratedObjects = totalMigratedObjects + numMigrated
        print(objectType + ": " + str(numMigrated) + "/" + str(len(allKeys)))
      print("--------------")
      if totalObjects == 0:
        print("No data to migrate")
      else:
        print("Migrated " + str(totalMigratedObjects) + "/" + str(totalObjects))
        mperc = (totalMigratedObjects / totalObjects) * 100
        print("You are " + "%0.2f" % (mperc,) + "% migrated")

      return None

    return storeTo.executeInsideTransaction(dbfn3)
  retVal = storeFrom.executeInsideTransaction(dbfn2)

  print("\n")

def getCommand():
  return {
    'Name': 'Caculate migration percentage',
    'fn': cmd
  }
