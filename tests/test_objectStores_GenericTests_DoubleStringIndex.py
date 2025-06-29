# Generic tests for doublestringindex
from object_store_abstraction import DoubleStringIndexClass, DoubleStringIndexInvalidKeyException, InvalidObjectTypeExceptionClass

def isThisTestToRun(nam, reqObjCon):
  if nam.startswith("t_"):
    return not reqObjCon
  if nam.startswith("tt_"):
    return reqObjCon
  return False

def runAllGenericTests(testClass, getObjFn, ConfigDict, tenantAweare=False):
  curModuleName = globals()['__name__']


  #globalsCopy = copy.deepcopy(globals())
  globalsCopy = []
  testsRequiringObjConsturctor = []
  for x in globals():
    if isThisTestToRun(x, False):
      globalsCopy.append(x)
    elif isThisTestToRun(x, True):
      testsRequiringObjConsturctor.append(x)
  for x in globalsCopy:
      # print("**********************************************************************")
      # print("    test " + x)
      # print("**********************************************************************")
      # print("")
      test_fn = globals()[x]
      obj = getObjFn(ConfigDict)
      def executeInsideTransactionFn(someFn):
          if tenantAweare:
              obj.executeInsideTransaction(tenantName="testTEn", fnToExecute=someFn)
          else:
              obj.executeInsideTransaction(fnToExecute=someFn)
      test_fn(testClass, executeInsideTransactionFn)
  for x in testsRequiringObjConsturctor:
      # print("**********************************************************************")
      # print("    test RQU CON" + x)
      # print("**********************************************************************")
      # print("")
      test_fn = globals()[x]
      test_fn(testClass, getObjFn, ConfigDict)


def t_saveAndRetrieveTwoStrings(testClass, objectStoreType_executeInsideTransaction):
    type = "test"
    doubleStringIndex = DoubleStringIndexClass(type)
    keys = [("KeyA_1","123abc"),("KeyA_2","222123abc"),("KeyA_3","alwaysremainB")]

    def someFn(connectionContext):
        for (keyA, keyB) in keys:
            doubleStringIndex.save(keyA, keyB, connectionContext)
            testClass.assertEqual(doubleStringIndex.getByA(keyA, connectionContext), keyB)
            testClass.assertEqual(doubleStringIndex.getByB(keyB, connectionContext), keyA)
    objectStoreType_executeInsideTransaction(someFn)

    def someFn2(connectionContext):
        for (keyA, keyB) in keys:
            testClass.assertEqual(doubleStringIndex.getByA(keyA, connectionContext), keyB)
            testClass.assertEqual(doubleStringIndex.getByB(keyB, connectionContext), keyA)
    objectStoreType_executeInsideTransaction(someFn2)

    def someFn3(connectionContext):
        doubleStringIndex.removeByA(keys[0][0], connectionContext)
        testClass.assertEqual(doubleStringIndex.getByA(keys[0][0], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByB(keys[0][1], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByA(keys[1][0], connectionContext), keys[1][1])
        testClass.assertEqual(doubleStringIndex.getByB(keys[1][1], connectionContext), keys[1][0])
        testClass.assertEqual(doubleStringIndex.getByA(keys[2][0], connectionContext), keys[2][1])
        testClass.assertEqual(doubleStringIndex.getByB(keys[2][1], connectionContext), keys[2][0])
    objectStoreType_executeInsideTransaction(someFn3)

    def someFn4(connectionContext):
        doubleStringIndex.removeByB(keys[1][1], connectionContext)
        testClass.assertEqual(doubleStringIndex.getByA(keys[0][0], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByB(keys[0][1], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByA(keys[1][0], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByB(keys[1][1], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByA(keys[2][0], connectionContext), keys[2][1])
        testClass.assertEqual(doubleStringIndex.getByB(keys[2][1], connectionContext), keys[2][0])
    objectStoreType_executeInsideTransaction(someFn4)

def t_willNotAcceptNonStringKeys(testClass, objectStoreType_executeInsideTransaction):
    doubleStringIndex = DoubleStringIndexClass("type")

    def someFn(connectionContext):
        with testClass.assertRaises(Exception) as context:
            doubleStringIndex.save("keyA", 123, connectionContext)
        with testClass.assertRaises(Exception) as context:
            doubleStringIndex.save(123, "keyB", connectionContext)
        with testClass.assertRaises(Exception) as context:
            doubleStringIndex.save("keyA", {}, connectionContext)
        with testClass.assertRaises(Exception) as context:
            doubleStringIndex.save({}, "keyB", connectionContext)
    objectStoreType_executeInsideTransaction(someFn)

def t_twotypesareseperate(testClass, objectStoreType_executeInsideTransaction):
    type1 = "test1"
    doubleStringIndex1 = DoubleStringIndexClass(type1)
    type2 = "test2"
    doubleStringIndex2 = DoubleStringIndexClass(type2)

    def someFn(connectionContext):
        doubleStringIndex1.save("a","1", connectionContext)
        doubleStringIndex2.save("b","2", connectionContext)
        testClass.assertEqual(doubleStringIndex1.getByA("a", connectionContext), "1")
        testClass.assertEqual(doubleStringIndex1.getByA("b", connectionContext), None)
        testClass.assertEqual(doubleStringIndex2.getByA("a", connectionContext), None)
        testClass.assertEqual(doubleStringIndex2.getByA("b", connectionContext), "2")

    objectStoreType_executeInsideTransaction(someFn)

def t_cannotstoenone(testClass, objectStoreType_executeInsideTransaction):
    doubleStringIndex1 = DoubleStringIndexClass("type")
    def someFn(connectionContext):
        with testClass.assertRaises(DoubleStringIndexInvalidKeyException) as context:
            doubleStringIndex1.save(None,"1", connectionContext)

        with testClass.assertRaises(DoubleStringIndexInvalidKeyException) as context:
            doubleStringIndex1.save("b",None, connectionContext)

    objectStoreType_executeInsideTransaction(someFn)

def t_canstoreemptystringaskey(testClass, objectStoreType_executeInsideTransaction):
    doubleStringIndex1 = DoubleStringIndexClass("type1")
    doubleStringIndex2 = DoubleStringIndexClass("type2")
    doubleStringIndex3 = DoubleStringIndexClass("type3")
    def someFn(connectionContext):
        doubleStringIndex1.save("", "1", connectionContext)
        testClass.assertEqual(doubleStringIndex1.getByA("", connectionContext), "1")
        testClass.assertEqual(doubleStringIndex1.getByA("1", connectionContext), None)
        testClass.assertEqual(doubleStringIndex1.getByB("", connectionContext), None)
        testClass.assertEqual(doubleStringIndex1.getByB("1", connectionContext), "")

        doubleStringIndex2.save("", "", connectionContext)
        testClass.assertEqual(doubleStringIndex2.getByA("", connectionContext), "")
        testClass.assertEqual(doubleStringIndex2.getByA("1", connectionContext), None)
        testClass.assertEqual(doubleStringIndex2.getByB("", connectionContext), "")
        testClass.assertEqual(doubleStringIndex2.getByB("1", connectionContext), None)

        doubleStringIndex3.save("1", "", connectionContext)
        testClass.assertEqual(doubleStringIndex3.getByA("", connectionContext), None)
        testClass.assertEqual(doubleStringIndex3.getByA("1", connectionContext), "")
        testClass.assertEqual(doubleStringIndex3.getByB("", connectionContext), "1")
        testClass.assertEqual(doubleStringIndex3.getByB("1", connectionContext), None)

    objectStoreType_executeInsideTransaction(someFn)

def t_willnotacceptinvalidobjectname(testClass, objectStoreType_executeInsideTransaction):
    idx = DoubleStringIndexClass("invalid_objname")

    def someFn(connectionContext):
        idx.save("", "1", connectionContext)

    with testClass.assertRaises(InvalidObjectTypeExceptionClass) as context:
        objectStoreType_executeInsideTransaction(someFn)

def t_truncateDoubleIndex(testClass, objectStoreType_executeInsideTransaction):
    doubleStringIndex1 = DoubleStringIndexClass("type1")
    doubleStringIndex2 = DoubleStringIndexClass("type2")
    doubleStringIndex3 = DoubleStringIndexClass("type3")

    def addRecords(idx):
        def a(connectionContext):
            idx.save("1", "A", connectionContext)
            idx.save("2", "B", connectionContext)
            idx.save("3", "C", connectionContext)
        return a

    def verifyRecords(idx):
        def a(connectionContext):
            testClass.assertEqual(idx.getByA("1", connectionContext), "A")
            testClass.assertEqual(idx.getByB("A", connectionContext), "1")
            testClass.assertEqual(idx.getByA("2", connectionContext), "B")
            testClass.assertEqual(idx.getByB("B", connectionContext), "2")
            testClass.assertEqual(idx.getByA("3", connectionContext), "C")
            testClass.assertEqual(idx.getByB("C", connectionContext), "3")
            testClass.assertEqual(idx.getByB("99", connectionContext), None)
            testClass.assertEqual(idx.getByB("Z", connectionContext), None)
        return a

    def truncate(idx):
        def a(connectionContext):
            idx.truncate(connectionContext)
        return a

    def verifyEmptyRecords(idx):
        def a(connectionContext):
            testClass.assertEqual(idx.getByA("1", connectionContext), None)
            testClass.assertEqual(idx.getByB("A", connectionContext), None)
            testClass.assertEqual(idx.getByA("2", connectionContext), None)
            testClass.assertEqual(idx.getByB("B", connectionContext), None)
            testClass.assertEqual(idx.getByA("3", connectionContext), None)
            testClass.assertEqual(idx.getByB("C", connectionContext), None)
            testClass.assertEqual(idx.getByB("99", connectionContext), None)
            testClass.assertEqual(idx.getByB("Z", connectionContext), None)
        return a

    objectStoreType_executeInsideTransaction(addRecords(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(addRecords(doubleStringIndex2))
    objectStoreType_executeInsideTransaction(addRecords(doubleStringIndex3))

    objectStoreType_executeInsideTransaction(verifyRecords(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(verifyRecords(doubleStringIndex2))
    objectStoreType_executeInsideTransaction(verifyRecords(doubleStringIndex3))

    objectStoreType_executeInsideTransaction(truncate(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(verifyEmptyRecords(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(verifyRecords(doubleStringIndex2))
    objectStoreType_executeInsideTransaction(verifyRecords(doubleStringIndex3))

    objectStoreType_executeInsideTransaction(truncate(doubleStringIndex2))
    objectStoreType_executeInsideTransaction(verifyEmptyRecords(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(verifyEmptyRecords(doubleStringIndex2))
    objectStoreType_executeInsideTransaction(verifyRecords(doubleStringIndex3))

    objectStoreType_executeInsideTransaction(truncate(doubleStringIndex3))
    objectStoreType_executeInsideTransaction(verifyEmptyRecords(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(verifyEmptyRecords(doubleStringIndex2))
    objectStoreType_executeInsideTransaction(verifyEmptyRecords(doubleStringIndex3))

    ## Truncate twice
    objectStoreType_executeInsideTransaction(truncate(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(truncate(doubleStringIndex1))

    ## Use again after truncate
    objectStoreType_executeInsideTransaction(addRecords(doubleStringIndex1))
    objectStoreType_executeInsideTransaction(verifyRecords(doubleStringIndex1))

