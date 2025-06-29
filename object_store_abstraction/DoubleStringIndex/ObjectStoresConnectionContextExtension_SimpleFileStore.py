from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase
import os
import json
import shutil #For remove dir and contents

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):

    def getDirstring(self, objectStoreTypeString):
        dirString = self.main_context.objectStore.baseLocation
        dirString += "/" + self.main_context.objectStore.directoryNamePrefixDoubleStringIndex
        dirString += self.main_context.objectStore.getFileSystemSafeStringFromKey(objectStoreTypeString)
        return dirString

    def getIndexFileName(self, objectStoreTypeString, byA):
        #This will ensure the file exists - initing it if required
        dirString = self.getDirstring(objectStoreTypeString)

        if not self.main_context.objectStore.isKnownIndexType(objectStoreTypeString):
            if not os.path.isdir(dirString):
                os.mkdir(dirString)
            self.main_context.objectStore.setKnownIndexType(objectStoreTypeString)

        if byA:
            dirString += "/byA.json"
        else:
            dirString += "/byB.json"
        if not os.path.isfile(dirString):
            with open(dirString, 'w') as target:  # 'w' mode overwrites file content
                target.write(json.dumps({}))
        return dirString

    def readIndex(self, fileName):
        with open(fileName, 'r') as source:
            data = json.load(source)
        return data

    def saveIndex(self, filename, idx):
        with open(filename, 'w') as target:  # 'w' mode overwrites file content
            target.write(json.dumps(idx))

    def save(self, objectStoreTypeString, keyA, keyB):
        with self.main_context.objectStore.fileAccessLock:
            indexAFilename = self.getIndexFileName(objectStoreTypeString, True)
            indexBFilename = self.getIndexFileName(objectStoreTypeString, False)
            byAIdx = self.readIndex(indexAFilename)
            byBIdx = self.readIndex(indexBFilename)
            byAIdx[keyA] = keyB
            byBIdx[keyB] = keyA
            self.saveIndex(indexAFilename, byAIdx)
            self.saveIndex(indexBFilename, byBIdx)

    def getByA(self, objectStoreTypeString, keyA):
        with self.main_context.objectStore.fileAccessLock:
            indexAFilename = self.getIndexFileName(objectStoreTypeString, True)
            byAIdx = self.readIndex(indexAFilename)
            if keyA not in byAIdx:
                return None
            return byAIdx[keyA]

    def getByB(self, objectStoreTypeString, keyB):
        with self.main_context.objectStore.fileAccessLock:
            indexBFilename = self.getIndexFileName(objectStoreTypeString, False)
            byBIdx = self.readIndex(indexBFilename)
            if keyB not in byBIdx:
                return None
            return byBIdx[keyB]

    def removeByA(self, objectStoreTypeString, keyA):
        with self.main_context.objectStore.fileAccessLock:
            indexAFilename = self.getIndexFileName(objectStoreTypeString, True)
            indexBFilename = self.getIndexFileName(objectStoreTypeString, False)
            byAIdx = self.readIndex(indexAFilename)
            byBIdx = self.readIndex(indexBFilename)
            keyB = byAIdx[keyA]
            del byAIdx[keyA]
            del byBIdx[keyB]
            self.saveIndex(indexAFilename, byAIdx)
            self.saveIndex(indexBFilename, byBIdx)

    def removeByB(self, objectStoreTypeString, keyB):
        with self.main_context.objectStore.fileAccessLock:
            indexAFilename = self.getIndexFileName(objectStoreTypeString, True)
            indexBFilename = self.getIndexFileName(objectStoreTypeString, False)
            byAIdx = self.readIndex(indexAFilename)
            byBIdx = self.readIndex(indexBFilename)
            keyA = byBIdx[keyB]
            del byAIdx[keyA]
            del byBIdx[keyB]
            self.saveIndex(indexAFilename, byAIdx)
            self.saveIndex(indexBFilename, byBIdx)

    def truncate(self, objectStoreTypeString):
        with self.main_context.objectStore.fileAccessLock:
            dirString = self.getDirstring(objectStoreTypeString)
            if os.path.exists(dirString):
                shutil.rmtree(dirString)
            self.main_context.objectStore.removeKnownIndexType(objectStoreTypeString)
