from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):
    def save(self, objectStoreTypeString, keyA, keyB):
        return self.main_context.toContext.doubleStringIndex.save(objectStoreTypeString, keyA, keyB)

    def getByA(self, objectStoreTypeString, keyA):
        new = self.main_context.toContext.doubleStringIndex.getByA(objectStoreTypeString, keyA)
        if new is not None:
            return new
        old = self.main_context.fromContext.doubleStringIndex.getByA(objectStoreTypeString, keyA)
        self.save(objectStoreTypeString, keyA, old)
        return old

    def getByB(self, objectStoreTypeString, keyB):
        new = self.main_context.toContext.doubleStringIndex.getByB(objectStoreTypeString, keyB)
        if new is not None:
            return new
        old = self.main_context.fromContext.doubleStringIndex.getByB(objectStoreTypeString, keyB)
        self.save(objectStoreTypeString, keyB, old)
        return old

    def removeByA(self, objectStoreTypeString, keyA):
        self.main_context.toContext.doubleStringIndex.removeByA(objectStoreTypeString, keyA)
        self.main_context.fromContext.doubleStringIndex.removeByA(objectStoreTypeString, keyA)

    def removeByB(self, objectStoreTypeString, keyB):
        self.main_context.toContext.doubleStringIndex.removeByB(objectStoreTypeString, keyB)
        self.main_context.fromContext.doubleStringIndex.removeByB(objectStoreTypeString, keyB)

    def truncate(self, objectStoreTypeString):
        self.main_context.toContext.doubleStringIndex.truncate(objectStoreTypeString)
        self.main_context.fromContext.doubleStringIndex.truncate(objectStoreTypeString)
