
ExceptionMsg = "ObjectStore DoubleStringIndex - Not overriden - "

class ObjectStoresConnectionContextExtensionBase():
    main_context = None
    def __init__(self, main_context):
        self.main_context = main_context

    def save(objectStoreTypeString, keyA, keyB):
        raise Exception(ExceptionMsg + "save")

    def getByA(objectStoreTypeString, keyA):
        raise Exception(ExceptionMsg + "getByA")

    def getByB(objectStoreTypeString, keyB):
        raise Exception(ExceptionMsg + "getByB")

    def removeByA(objectStoreTypeString, keyA):
        raise Exception(ExceptionMsg + "removeByA")

    def removeByB(objectStoreTypeString, keyB):
        raise Exception(ExceptionMsg + "removeByB")
