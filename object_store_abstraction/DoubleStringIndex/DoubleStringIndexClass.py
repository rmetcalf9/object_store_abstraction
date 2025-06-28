
class DoubleStringIndexInvalidKeyException(Exception):
    pass

class DoubleStringIndexClass():
    # Like a repository but much simpler
    objectStoreTypeString = None

    def __init__(self, objectStoreTypeString):
        self.objectStoreTypeString = objectStoreTypeString

    def save(self, keyA, keyB, storeConnection):
        storeConnection.validateObjectType(self.objectStoreTypeString)
        if keyA is None:
            raise DoubleStringIndexInvalidKeyException("Invalid Key A Type - Can not be none")
        if keyB is None:
            raise DoubleStringIndexInvalidKeyException("Invalid Key B Type - Can not be none")
        if not isinstance(keyA, str):
            raise DoubleStringIndexInvalidKeyException("Invalid Key A Type", type(keyA))
        if not isinstance(keyB, str):
            raise DoubleStringIndexInvalidKeyException("Invalid Key B Type", type(keyB))
        if len(keyA) > 140:
            raise DoubleStringIndexInvalidKeyException("Keys can not be more than 140 chars")
        if len(keyB) > 140:
            raise DoubleStringIndexInvalidKeyException("Keys can not be more than 140 chars")
        return storeConnection.doubleStringIndex.save(objectStoreTypeString=self.objectStoreTypeString, keyA=keyA, keyB=keyB)

    def getByA(self,  keyA, storeConnection):
        return storeConnection.doubleStringIndex.getByA(self.objectStoreTypeString, keyA)

    def getByB(self, keyB, storeConnection):
        return storeConnection.doubleStringIndex.getByB(self.objectStoreTypeString, keyB)

    def removeByA(self, keyA, storeConnection):
        return storeConnection.doubleStringIndex.removeByA(self.objectStoreTypeString, keyA)

    def removeByB(self, keyB, storeConnection):
        return storeConnection.doubleStringIndex.removeByB(self.objectStoreTypeString, keyB)

    def truncate(self, storeConnection):
        return storeConnection.doubleStringIndex.truncate(self.objectStoreTypeString)
