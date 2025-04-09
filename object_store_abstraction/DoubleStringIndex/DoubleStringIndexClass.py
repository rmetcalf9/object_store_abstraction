

class DoubleStringIndexClass():
    # Like a repository but much simpler
    objectStoreTypeString = None

    def __init__(self, objectStoreTypeString):
        self.objectStoreTypeString = objectStoreTypeString

    def save(self, keyA, keyB, storeConnection):
        if not isinstance(keyA, str):
            raise Exception("Invalid Key A Type", type(keyA))
        if not isinstance(keyB, str):
            raise Exception("Invalid Key B Type", type(keyB))
        return storeConnection.doubleStringIndex.save(objectStoreTypeString=self.objectStoreTypeString, keyA=keyA, keyB=keyB)

    def getByA(self,  keyA, storeConnection):
        return storeConnection.doubleStringIndex.getByA(self.objectStoreTypeString, keyA)

    def getByB(self, keyB, storeConnection):
        return storeConnection.doubleStringIndex.getByB(self.objectStoreTypeString, keyB)

    def removeByA(self, keyA, storeConnection):
        return storeConnection.doubleStringIndex.removeByA(self.objectStoreTypeString, keyA)

    def removeByB(self, keyB, storeConnection):
        return storeConnection.doubleStringIndex.removeByB(self.objectStoreTypeString, keyB)


