import copy
from .RepositoryBaseClass import RepositoryBaseClass

class RepositoryObjBaseClass():
  obj = None
  repositoryObj = None

  # Common model in all objects but never saved to DB
  @classmethod
  def getMetadataModel(cls, appObj, flaskrestplusfields):
    return appObj.flastRestPlusAPIObject.model('RepositoryObjectBaseClass Metadata', {
      'objectVersion': flaskrestplusfields.String(default='DEFAULT', description='Object version required to sucessfully preform updates'),
      'creationDateTime': flaskrestplusfields.DateTime(dt_format=u'iso8601', description='Datetime user was created'),
      'lastUpdateDateTime': flaskrestplusfields.DateTime(dt_format=u'iso8601', description='Datetime user was lastupdated'),
      'objectKey': flaskrestplusfields.String(default='DEFAULT', description='Unique key used in datastore')
    })
  @classmethod
  def getMetadataElementKey(cls):
    return "metadata"

  def __init__(self, obj, objVersion, creationDateTime, lastUpdateDateTime, objKey, repositoryObj):
    self.obj = copy.deepcopy(obj)
    self.repositoryObj = repositoryObj
    if RepositoryObjBaseClass.getMetadataElementKey() in self.obj:
      del self.obj["metadata"]
      # raise RepositoryBaseClass.validationException(RepositoryObjBaseClass.getMetadataElementKey() + " tag must not be present in object")
    self.obj[RepositoryObjBaseClass.getMetadataElementKey()] = {
      "objectVersion": objVersion,
      "creationDateTime": creationDateTime.isoformat(),
      "lastUpdateDateTime": lastUpdateDateTime.isoformat(),
      "objectKey": objKey
    }

  def getDict(self):
    return self.obj

  def save(self, storeConnection):
    objID, objectVersion = self.repositoryObj.upsert(
      obj=self.getDict(),
      objectVersion=self.getDict()[RepositoryObjBaseClass.getMetadataElementKey()]["objectVersion"],
      storeConnection=storeConnection
    )
    return (objID, objectVersion)
