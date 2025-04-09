from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, DateTime, JSON, func, UniqueConstraint, and_, Text, select

class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):
    def save(self, objectStoreTypeString, keyA, keyB):
        queryDelA = self.main_context.objectStore.idxDataTable.delete(whereclause=(
            and_(
                self.main_context.objectStore.idxDataTable.c.type == objectStoreTypeString,
                self.main_context.objectStore.idxDataTable.c.keyA == keyA
            )
        ))
        resultDelA = self.main_context._INT_execute(queryDelA)
        queryDelB = self.main_context.objectStore.idxDataTable.delete(whereclause=(
            and_(
                self.main_context.objectStore.idxDataTable.c.type == objectStoreTypeString,
                self.main_context.objectStore.idxDataTable.c.keyB == keyB
            )
        ))
        resultDelB = self.main_context._INT_execute(queryDelB)
        main_query = self.main_context.objectStore.idxDataTable.insert().values(
            type=objectStoreTypeString,
            keyA=keyA,
            keyB=keyB
        )
        resultMain = self.main_context._INT_execute(main_query)
        if len(resultMain.inserted_primary_key) != 1:
            raise Exception('_DoubleStringIndexConnectionContextExtension Save wrong number of rows inserted')

    def getByA(self, objectStoreTypeString, keyA):
        whereclause = and_(
            self.main_context.objectStore.idxDataTable.c.type == objectStoreTypeString,
            self.main_context.objectStore.idxDataTable.c.keyA == keyA
        )

        query = select([self.main_context.objectStore.idxDataTable.c.keyB]).where(whereclause)
        result = self.main_context._INT_execute(query)
        firstRow = result.fetchone()
        if firstRow is None:
            return None
        if result.rowcount != 1:
            raise Exception('_DoubleStringIndexConnectionContextExtension getByA Wrong number of rows returned for key')
        return firstRow[0]

    def getByB(self, objectStoreTypeString, keyB):
        whereclause = and_(
            self.main_context.objectStore.idxDataTable.c.type == objectStoreTypeString,
            self.main_context.objectStore.idxDataTable.c.keyB == keyB
        )

        query = select([self.main_context.objectStore.idxDataTable.c.keyA]).where(whereclause)
        result = self.main_context._INT_execute(query)
        firstRow = result.fetchone()
        if firstRow is None:
            return None
        if result.rowcount != 1:
            raise Exception('_DoubleStringIndexConnectionContextExtension getByA Wrong number of rows returned for key')
        return firstRow[0]

    def removeByA(self, objectStoreTypeString, keyA):
        queryDelA = self.main_context.objectStore.idxDataTable.delete(whereclause=(
            and_(
                self.main_context.objectStore.idxDataTable.c.type == objectStoreTypeString,
                self.main_context.objectStore.idxDataTable.c.keyA == keyA
            )
        ))
        resultDelA = self.main_context._INT_execute(queryDelA)

    def removeByB(self, objectStoreTypeString, keyB):
        queryDelB = self.main_context.objectStore.idxDataTable.delete(whereclause=(
            and_(
                self.main_context.objectStore.idxDataTable.c.type == objectStoreTypeString,
                self.main_context.objectStore.idxDataTable.c.keyB == keyB
            )
        ))
        resultDelA = self.main_context._INT_execute(queryDelB)
