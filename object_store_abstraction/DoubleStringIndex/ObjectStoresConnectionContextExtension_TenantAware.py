from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):
    def save(self, objectStoreTypeString, keyA, keyB):
        return self.main_context.objectStoreContext.doubleStringIndex.save(
            objectStoreTypeString=self.main_context.INT__getDirivedObjectType(objectStoreTypeString),
            keyA=keyA,
            keyB=keyB
        )

    def getByA(self, objectStoreTypeString, keyA):
        return self.main_context.objectStoreContext.doubleStringIndex.getByA(
            objectStoreTypeString=self.main_context.INT__getDirivedObjectType(objectStoreTypeString),
            keyA=keyA
        )

    def getByB(self, objectStoreTypeString, keyB):
        return self.main_context.objectStoreContext.doubleStringIndex.getByB(
            objectStoreTypeString=self.main_context.INT__getDirivedObjectType(objectStoreTypeString),
            keyB=keyB
        )

    def removeByA(self, objectStoreTypeString, keyA):
        return self.main_context.objectStoreContext.doubleStringIndex.removeByA(
            objectStoreTypeString=self.main_context.INT__getDirivedObjectType(objectStoreTypeString),
            keyA=keyA
        )

    def removeByB(self, objectStoreTypeString, keyB):
        return self.main_context.objectStoreContext.doubleStringIndex.removeByB(
            objectStoreTypeString=self.main_context.INT__getDirivedObjectType(objectStoreTypeString),
            keyB=keyB
        )

    def truncate(self, objectStoreTypeString):
        return self.main_context.objectStoreContext.doubleStringIndex.truncate(
            objectStoreTypeString=self.main_context.INT__getDirivedObjectType(objectStoreTypeString)
        )
