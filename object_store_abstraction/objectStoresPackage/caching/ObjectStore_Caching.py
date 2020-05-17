from object_store_abstraction import ObjectStore
from .ConnectionContext import ConnectionContext


class ObjectStore_Caching(ObjectStore):
  def __init__(self, configJSON, externalFns, detailLogging, type, factoryFn):
    super(ObjectStore_Caching, self).__init__(externalFns, detailLogging, type)

  def _getConnectionContext(self):
    return ConnectionContext(self)