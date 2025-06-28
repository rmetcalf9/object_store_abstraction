from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):

    def save(self, objectStoreTypeString, keyA, keyB):
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndex()
        if objectStoreTypeString not in idx:
            idx[objectStoreTypeString] = {
                "byA": {},
                "byB": {}
            }
        idx[objectStoreTypeString]["byA"][keyA] = keyB
        idx[objectStoreTypeString]["byB"][keyB] = keyA

    def getByA(self, objectStoreTypeString, keyA):
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndex()
        if objectStoreTypeString not in idx:
            return None
        if keyA not in idx[objectStoreTypeString]["byA"]:
            return None
        return idx[objectStoreTypeString]["byA"][keyA]

    def getByB(self, objectStoreTypeString, keyB):
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndex()
        if objectStoreTypeString not in idx:
            return None
        if keyB not in idx[objectStoreTypeString]["byB"]:
            return None
        return idx[objectStoreTypeString]["byB"][keyB]

    def removeByA(self, objectStoreTypeString, keyA):
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndex()
        if objectStoreTypeString not in idx:
            return
        keyB = idx[objectStoreTypeString]["byA"][keyA]
        del idx[objectStoreTypeString]["byA"][keyA]
        del idx[objectStoreTypeString]["byB"][keyB]

    def removeByB(self, objectStoreTypeString, keyB):
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndex()
        if objectStoreTypeString not in idx:
            return
        keyA = idx[objectStoreTypeString]["byB"][keyB]
        del idx[objectStoreTypeString]["byA"][keyA]
        del idx[objectStoreTypeString]["byB"][keyB]

    def truncate(self, objectStoreTypeString):
        idx = self.main_context.objectStore._INT_getDictForDoubleStringIndex()
        if objectStoreTypeString not in idx:
            return
        del idx[objectStoreTypeString]
