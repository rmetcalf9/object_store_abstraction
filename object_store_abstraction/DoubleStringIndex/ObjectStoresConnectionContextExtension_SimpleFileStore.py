from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):

    def save(self, objectStoreTypeString, keyA, keyB):
        raise Exception("DD")

