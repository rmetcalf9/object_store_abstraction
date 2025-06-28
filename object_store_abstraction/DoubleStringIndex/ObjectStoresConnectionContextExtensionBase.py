
ExceptionMsg = "ObjectStore DoubleStringIndex - Not overridden - "

class ObjectStoresConnectionContextExtensionBase():
    main_context = None
    def __init__(self, main_context):
        self.main_context = main_context

    def save(self, objectStoreTypeString, keyA, keyB):
        raise Exception(ExceptionMsg + "save")

    def getByA(self, objectStoreTypeString, keyA):
        raise Exception(ExceptionMsg + "getByA")

    def getByB(self, objectStoreTypeString, keyB):
        raise Exception(ExceptionMsg + "getByB")

    def removeByA(self, objectStoreTypeString, keyA):
        raise Exception(ExceptionMsg + "removeByA")

    def removeByB(self, objectStoreTypeString, keyB):
        raise Exception(ExceptionMsg + "removeByB")

    def truncate(self, objectStoreTypeString):
        raise Exception(ExceptionMsg + "truncate")
