from .ObjectStoresConnectionContextExtensionBase import ObjectStoresConnectionContextExtensionBase
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, DateTime, JSON, \
    func, UniqueConstraint, and_, Text, select, delete, insert


class DoubleStringIndexConnectionContextExtension(ObjectStoresConnectionContextExtensionBase):
    def save(self, objectStoreTypeString, keyA, keyB):
        idxTable = self.main_context.objectStore.idxDataTable
        queryDelA = (
            delete(idxTable)  # Use the standalone delete() function
            .where(  # Use the chainable .where() method
                and_(
                    idxTable.c.type == objectStoreTypeString,
                    idxTable.c.keyA == keyA
                )
            )
        )
        resultDelA = self.main_context._INT_execute(queryDelA)
        queryDelB = (
            delete(idxTable)  # Use the standalone delete() function
            .where(  # Use the chainable .where() method
                and_(
                    idxTable.c.type == objectStoreTypeString,
                    idxTable.c.keyA == keyB
                )
            )
        )
        resultDelB = self.main_context._INT_execute(queryDelB)
        main_query = (
            insert(idxTable)  # Use the standalone insert() function, passing the table
            .values(  # Use the .values() method to specify columns and values
                type=objectStoreTypeString,
                keyA=keyA,
                keyB=keyB
            )
        )
        resultMain = self.main_context._INT_execute(main_query)
        if len(resultMain.inserted_primary_key) != 1:
            raise Exception('_DoubleStringIndexConnectionContextExtension Save wrong number of rows inserted')

    def getByA(self, objectStoreTypeString, keyA):
        table = self.main_context.objectStore.idxDataTable
        type_column = table.c.type
        keyA_column = table.c.keyA
        keyB_column = table.c.keyB
        whereclause = and_(
            type_column == objectStoreTypeString,
            keyA_column == keyA
        )
        query = (
            select(keyB_column)  # Pass the column directly, without list brackets
            .where(whereclause)  # Use the chainable .where() method
        )
        result = self.main_context._INT_execute(query)
        firstRow = result.fetchone()
        if firstRow is None:
            return None
        if result.rowcount != 1:
            raise Exception('_DoubleStringIndexConnectionContextExtension getByA Wrong number of rows returned for key')
        return firstRow[0]

    def getByB(self, objectStoreTypeString, keyB):
        table = self.main_context.objectStore.idxDataTable
        type_column = table.c.type
        keyA_column = table.c.keyA
        keyB_column = table.c.keyB
        whereclause = and_(
            type_column == objectStoreTypeString,
            keyB_column == keyB
        )
        query = (
            select(keyA_column)  # Pass the column directly, without list brackets
            .where(whereclause)  # Use the chainable .where() method
        )
        result = self.main_context._INT_execute(query)
        firstRow = result.fetchone()
        if firstRow is None:
            return None
        if result.rowcount != 1:
            raise Exception('_DoubleStringIndexConnectionContextExtension getByB Wrong number of rows returned for key')
        return firstRow[0]

    def removeByA(self, objectStoreTypeString, keyA):
        table = self.main_context.objectStore.idxDataTable
        type_column = table.c.type
        keyA_column = table.c.keyA
        whereclause = and_(
            type_column == objectStoreTypeString,
            keyA_column == keyA
        )
        queryDelA = (
            delete(table)  # 1. Use the standalone delete() function
            .where(whereclause)
        )
        resultDelA = self.main_context._INT_execute(queryDelA)

    def removeByB(self, objectStoreTypeString, keyB):
        table = self.main_context.objectStore.idxDataTable
        type_column = table.c.type
        keyB_column = table.c.keyB
        whereclause = and_(
            type_column == objectStoreTypeString,
            keyB_column == keyB
        )
        queryDelB = (
            delete(table)  # 1. Use the standalone delete() function
            .where(whereclause)
        )
        resultDelA = self.main_context._INT_execute(queryDelB)

    def truncate(self, objectStoreTypeString):
        table = self.main_context.objectStore.idxDataTable
        type_column = table.c.type
        queryTruncate = (
            delete(table)  # Use the standalone delete() function
            .where(  # Use the chainable .where() method
                type_column == objectStoreTypeString
            )
        )
        resultTruncate = self.main_context._INT_execute(queryTruncate)

