from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase

objectKeyForKeyA="a4tgsDoubleStringIndetExtensieyskeyA"
objectKeyForKeyB="a4tgsDoubleStringIndetExtensieyskeyB"

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):
    def save(self, objectStoreTypeString, keyA, keyB):
        (objectDICTA, ObjectVersionA, creationDateA, lastUpdateDateA, objectKeyA) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyA
        )
        if objectDICTA is None:
            objectDICTA = {}
        (objectDICTB, ObjectVersionB, creationDateB, lastUpdateDateB, objectKeyB) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyB
        )
        if objectDICTB is None:
            objectDICTB = {}
        objectDICTA[keyA] = keyB
        objectDICTB[keyB] = keyA
        (_, _, _) = self.main_context.saveJSONObjectV2(
            objectType=objectStoreTypeString,
            objectKey=objectKeyForKeyA,
            JSONString=objectDICTA,
            objectVersion=ObjectVersionA
        )
        (_, _, _) = self.main_context.saveJSONObjectV2(
            objectType=objectStoreTypeString,
            objectKey=objectKeyForKeyB,
            JSONString=objectDICTB,
            objectVersion=ObjectVersionB
        )

    def getByA(self, objectStoreTypeString, keyA):
        (objectDICTA, ObjectVersionA, creationDateA, lastUpdateDateA, objectKeyA) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyA
        )
        if objectDICTA is None:
            return None
        if keyA not in objectDICTA:
            return None
        if objectDICTA[keyA] is None:
            return None
        return objectDICTA[keyA]

    def getByB(self, objectStoreTypeString, keyB):
        (objectDICTB, ObjectVersionB, creationDateB, lastUpdateDateB, objectKeyB) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyB
        )
        if objectDICTB is None:
            return None
        if keyB not in objectDICTB:
            return None
        if objectDICTB[keyB] is None:
            return None
        return objectDICTB[keyB]

    def removeByA(self, objectStoreTypeString, keyA):
        (objectDICTA, ObjectVersionA, creationDateA, lastUpdateDateA, objectKeyA) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyA
        )
        if objectDICTA is None:
            objectDICTA = {}
        (objectDICTB, ObjectVersionB, creationDateB, lastUpdateDateB, objectKeyB) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyB
        )
        if objectDICTB is None:
            objectDICTB = {}
        keyB = objectDICTA[keyA]
        del objectDICTA[keyA]
        del objectDICTB[keyB]
        (_, _, _) = self.main_context.saveJSONObjectV2(
            objectType=objectStoreTypeString,
            objectKey=objectKeyForKeyA,
            JSONString=objectDICTA,
            objectVersion=ObjectVersionA
        )
        (_, _, _) = self.main_context.saveJSONObjectV2(
            objectType=objectStoreTypeString,
            objectKey=objectKeyForKeyB,
            JSONString=objectDICTB,
            objectVersion=ObjectVersionB
        )

    def removeByB(self, objectStoreTypeString, keyB):
        (objectDICTA, ObjectVersionA, creationDateA, lastUpdateDateA, objectKeyA) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyA
        )
        if objectDICTA is None:
            objectDICTA = {}
        (objectDICTB, ObjectVersionB, creationDateB, lastUpdateDateB, objectKeyB) = self.main_context.getObjectJSON(
            objectStoreTypeString,
            objectKeyForKeyB
        )
        if objectDICTB is None:
            objectDICTB = {}
        keyA = objectDICTB[keyB]
        del objectDICTA[keyA]
        del objectDICTB[keyB]
        (_, _, _) = self.main_context.saveJSONObjectV2(
            objectType=objectStoreTypeString,
            objectKey=objectKeyForKeyA,
            JSONString=objectDICTA,
            objectVersion=ObjectVersionA
        )
        (_, _, _) = self.main_context.saveJSONObjectV2(
            objectType=objectStoreTypeString,
            objectKey=objectKeyForKeyB,
            JSONString=objectDICTB,
            objectVersion=ObjectVersionB
        )

    def truncate(self, objectStoreTypeString):
        self.main_context.truncateObjectType(objectStoreTypeString)
