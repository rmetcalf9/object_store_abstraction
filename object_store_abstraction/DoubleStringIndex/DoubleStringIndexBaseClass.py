

class DoubleStringIndexBaseClass():
    # Like a repository but much simpler
    objectStoreTypeString = None

    def __init__(self, objectStoreTypeString):
        self.objectStoreTypeString = objectStoreTypeString

    def save(keyA, keyB, storeConnection):
        return storeConnection.doubleStringIndex.save(self.objectStoreTypeString, keyA, keyB)

    def getByA(keyA, storeConnection):
        return storeConnection.doubleStringIndex.getByA(self.objectStoreTypeString, keyA)

    def getByB(keyB, storeConnection):
        return storeConnection.doubleStringIndex.getByB(self.objectStoreTypeString, keyB)

    def removeByA(keyA, storeConnection):
        return storeConnection.doubleStringIndex.removeByA(self.objectStoreTypeString, keyA)

    def removeByB(keyB, storeConnection):
        return storeConnection.doubleStringIndex.removeByB(self.objectStoreTypeString, keyB)


