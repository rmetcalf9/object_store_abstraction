# Generic tests for doublestringindex
from object_store_abstraction import DoubleStringIndexClass

def isThisTestToRun(nam, reqObjCon):
  if nam.startswith("t_"):
    return not reqObjCon
  if nam.startswith("tt_"):
    return reqObjCon
  return False

def runAllGenericTests(testClass, getObjFn, ConfigDict):
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
      test_fn(testClass, obj)
  for x in testsRequiringObjConsturctor:
      # print("**********************************************************************")
      # print("    test RQU CON" + x)
      # print("**********************************************************************")
      # print("")
      test_fn = globals()[x]
      test_fn(testClass, getObjFn, ConfigDict)


def t_saveAndRetrieveTwoStrings(testClass, objectStoreType):
    type = "test"
    doubleStringIndex = DoubleStringIndexClass(type)
    keys = [("KeyA_1","123abc"),("KeyA_2","222123abc"),("KeyA_3","alwaysremainB")]

    def someFn(connectionContext):
        for (keyA, keyB) in keys:
            doubleStringIndex.save(keyA, keyB, connectionContext)
            testClass.assertEqual(doubleStringIndex.getByA(keyA, connectionContext), keyB)
            testClass.assertEqual(doubleStringIndex.getByB(keyB, connectionContext), keyA)
    objectStoreType.executeInsideTransaction(someFn)

    def someFn2(connectionContext):
        for (keyA, keyB) in keys:
            testClass.assertEqual(doubleStringIndex.getByA(keyA, connectionContext), keyB)
            testClass.assertEqual(doubleStringIndex.getByB(keyB, connectionContext), keyA)
    objectStoreType.executeInsideTransaction(someFn2)

    def someFn3(connectionContext):
        doubleStringIndex.removeByA(keys[0][0], connectionContext)
        testClass.assertEqual(doubleStringIndex.getByA(keys[0][0], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByB(keys[0][1], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByA(keys[1][0], connectionContext), keys[1][1])
        testClass.assertEqual(doubleStringIndex.getByB(keys[1][1], connectionContext), keys[1][0])
        testClass.assertEqual(doubleStringIndex.getByA(keys[2][0], connectionContext), keys[2][1])
        testClass.assertEqual(doubleStringIndex.getByB(keys[2][1], connectionContext), keys[2][0])
    objectStoreType.executeInsideTransaction(someFn3)

    def someFn4(connectionContext):
        doubleStringIndex.removeByB(keys[1][1], connectionContext)
        testClass.assertEqual(doubleStringIndex.getByA(keys[0][0], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByB(keys[0][1], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByA(keys[1][0], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByB(keys[1][1], connectionContext), None)
        testClass.assertEqual(doubleStringIndex.getByA(keys[2][0], connectionContext), keys[2][1])
        testClass.assertEqual(doubleStringIndex.getByB(keys[2][1], connectionContext), keys[2][0])
    objectStoreType.executeInsideTransaction(someFn4)

def t_willNotAcceptNonStringKeys(testClass, objectStoreType):
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
    objectStoreType.executeInsideTransaction(someFn)

def t_twotypesareseperate(testClass, objectStoreType):
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

    objectStoreType.executeInsideTransaction(someFn)


