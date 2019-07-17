from TestHelperSuperClass import testHelperSuperClass

class objectStoresWithPrefix(testHelperSuperClass):
  def differentPrefixesDontShareData(self, testClass, objectStoreType, objectStoreType2):
    def dbfn(storeConnection):
      def dbfn2(storeConnection2):

        def someFn(connectionContext):
          for x in range(1,6):
            connectionContext.saveJSONObject("Test", "1_123" + str(x), JSONString, None)
        def someFn2(connectionContext2):
          for x in range(1,6):
            connectionContext2.saveJSONObject("Test", "2_123" + str(x), JSONString, None)

        storeConnection.executeInsideTransaction(someFn)
        storeConnection2.executeInsideTransaction(someFn2)

        for x in range(1,6):
          (objectDICT, ObjectVersion, creationDate, lastUpdateDate) = storeConnection.getObjectJSON("Test", "1_123" + str(x))
          testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='Saved object dosen\'t match')
          (objectDICT, ObjectVersion, creationDate, lastUpdateDate) = storeConnection2.getObjectJSON("Test", "1_123" + str(x))
          testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='Saved object dosen\'t match')

          (objectDICT, ObjectVersion, creationDate, lastUpdateDate) = storeConnection.getObjectJSON("Test", "2_123" + str(x))
          testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, None, [  ], msg='Saved object dosen\'t match')
          (objectDICT, ObjectVersion, creationDate, lastUpdateDate) = storeConnection2.getObjectJSON("Test", "2_123" + str(x))
          testClass.assertJSONStringsEqualWithIgnoredKeys(objectDICT, JSONString, [  ], msg='Saved object dosen\'t match')
      objectStoreType2.executeInsideConnectionContext(dbfn2)
    objectStoreType.executeInsideConnectionContext(dbfn)


JSONString = {
  'AA': "AA",
  'BB': "BB",
  "CC": {
    "CC.AA": "AA",
    "CC.BB": "BB",
    "CC.CC": "CC"
  }
}
JSONString2 = {
  'AA': "AA2",
  'BB': "BB2",
  "CC": {
    "CC.AA": "AA2",
    "CC.BB": "BB2",
    "CC.CC": "CC2"
  }
}
