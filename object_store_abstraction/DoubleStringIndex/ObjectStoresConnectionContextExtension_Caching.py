from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):
    def save(self, objectStoreTypeString, keyA, keyB):
        self.main_context.mainContext.doubleStringIndex.save(objectStoreTypeString, keyA, keyB)
        # Add it to the cache - index caches never expire
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndexCache()
        if objectStoreTypeString not in idx:
            idx[objectStoreTypeString] = {
                "byA": {},
                "byB": {}
            }
        idx[objectStoreTypeString]["byA"][keyA] = keyB
        idx[objectStoreTypeString]["byB"][keyB] = keyA


    def getByA(self, objectStoreTypeString, keyA):
        # Check Cache first, then if we get none get from store
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndexCache()
        if objectStoreTypeString not in idx:
            return self.main_context.mainContext.doubleStringIndex.getByA(objectStoreTypeString, keyA)
        if keyA not in idx[objectStoreTypeString]["byA"]:
            return self.main_context.mainContext.doubleStringIndex.getByA(objectStoreTypeString, keyA)
        ret_val = idx[objectStoreTypeString]["byA"][keyA]
        if ret_val is None:
            return self.main_context.mainContext.doubleStringIndex.getByA(objectStoreTypeString, keyA)
        return ret_val

    def getByB(self, objectStoreTypeString, keyB):
        # Check Cache first, then if we get none get from store
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndexCache()
        if objectStoreTypeString not in idx:
            return self.main_context.mainContext.doubleStringIndex.getByB(objectStoreTypeString, keyB)
        if keyB not in idx[objectStoreTypeString]["byB"]:
            return self.main_context.mainContext.doubleStringIndex.getByB(objectStoreTypeString, keyB)
        ret_val = idx[objectStoreTypeString]["byB"][keyB]
        if ret_val is None:
            return self.main_context.mainContext.doubleStringIndex.getByB(objectStoreTypeString, keyB)
        return ret_val

    def removeByA(self, objectStoreTypeString, keyA):
        #Remove from both cache and main store
        self.main_context.mainContext.doubleStringIndex.removeByA(objectStoreTypeString, keyA)

        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndexCache()
        if objectStoreTypeString not in idx:
            return
        keyB = idx[objectStoreTypeString]["byA"][keyA]
        del idx[objectStoreTypeString]["byA"][keyA]
        del idx[objectStoreTypeString]["byB"][keyB]

    def removeByB(self, objectStoreTypeString, keyB):
        #Remove from both cache and main store
        self.main_context.mainContext.doubleStringIndex.removeByB(objectStoreTypeString, keyB)

        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndexCache()
        if objectStoreTypeString not in idx:
            return
        keyA = idx[objectStoreTypeString]["byB"][keyB]
        del idx[objectStoreTypeString]["byA"][keyA]
        del idx[objectStoreTypeString]["byB"][keyB]

    def truncate(self, objectStoreTypeString):
        self.main_context.mainContext.doubleStringIndex.truncate(objectStoreTypeString)
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndexCache()
        if objectStoreTypeString not in idx:
            return
        del idx[objectStoreTypeString]
