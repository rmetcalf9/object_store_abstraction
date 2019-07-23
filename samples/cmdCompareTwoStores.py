import utils


def compareTwoStores():
  storeL = utils.getAnObjectStoreFromUser("Select Left Store to compare")
  storeR = utils.getAnObjectStoreFromUser("Select Right Store to compare")

  print("Reading Data Store...")
  def dbfn2(storeConnectionL):
    def dbfn3(storeConnectionR):
      data = []
      objectTypesL = storeConnectionL.list_all_objectTypes()
      objectTypesR = storeConnectionR.list_all_objectTypes()



      for objectType in objectTypesL:
        allKeysL = storeConnectionL.getAllRowsForObjectType(
          objectType=objectType,
          filterFN=None,
          outputFN=utils.outputFnJustKeys,
          whereClauseText=None
        )

        stats = {
          "Name        ": objectType,
          "InLeft": True,
          "InRight": False,
          "t_RowsLeftOnly": -1,
          "t_RowsRightOnly": -1,
          "t_RowsBoth": -1,
          "t_RowsLeft": len(allKeysL),
          "t_RowsRight": -1
        }
        if objectType in objectTypesR:
          stats["InRight"] = True
          allKeysR = storeConnectionR.getAllRowsForObjectType(
            objectType=objectType,
            filterFN=None,
            outputFN=utils.outputFnJustKeys,
            whereClauseText=None
          )
          stats["t_RowsLeftOnly"] = 0
          stats["t_RowsRightOnly"] = 0
          stats["t_RowsBoth"] = 0

          for key in allKeysL:
            if key in allKeysR:
              stats["t_RowsBoth"] = stats["t_RowsBoth"] + 1
            else:
              stats["t_RowsLeftOnly"] = stats["t_RowsLeftOnly"] + 1

          for key in allKeysR:
            if key not in allKeysL:
              stats["t_RowsRightOnly"] = stats["t_RowsRightOnly"] + 1

          stats["t_RowsRight"] = len(allKeysR)

        else:
          stats["t_RowsLeftOnly"] = len(allKeysL)
          stats["t_RowsRightOnly"] = 0
          stats["t_RowsBoth"] = 0
          stats["t_RowsRight"] = 0

        data.append(stats)

      #Add objectTypes in Right not left
      for objectType in objectTypesR:
        if objectType not in objectTypesL:
          #Not covered
          allKeysR = storeConnectionL.getAllRowsForObjectType(
            objectType=objectType,
            filterFN=None,
            outputFN=outputFnJustKeys,
            whereClauseText=None
          )
          stats = {
            "Name        ": objectType,
            "InLeft": False,
            "InRight": True,
            "t_RowsLeftOnly": 0,
            "t_RowsRightOnly": len(allKeysR),
            "t_RowsBoth": 0,
            "t_RowsLeft": 0,
            "t_RowsRight": len(allKeysR)
          }
          data.append(stats)

      return data
    return storeR.executeInsideTransaction(dbfn3)
  data = storeL.executeInsideTransaction(dbfn2)

  print("\n")
  if len(data)==0:
    print("No Data")
    return

  print("Left is ", storeL.type)
  print("Right is ", storeR.type)


  keysInOrder = [
    "Name        ",
    "InLeft",
    "t_RowsLeft",
    "t_RowsLeftOnly",
    "t_RowsBoth",
    "t_RowsRightOnly",
    "t_RowsRight",
    "InRight"
  ]

  for k in keysInOrder:
    print(k, end="")
    print(" ", end="")
  print("")
  for k in keysInOrder:
    for x in range(len(k)):
      print("-", end="")
    print(" ", end="")
  print("")

  for x in data:
    for k in keysInOrder:
      print((str(x[k]) + utils.space)[:len(k)] + " ", end="")
    print("")

  print("---\n")


def getCommand():
  return {
    'Name': 'Compare Two Stores',
    'fn': compareTwoStores
  }
