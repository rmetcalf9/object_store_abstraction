import uuid
import copy

class RepositoryValidationException(Exception):
  pass

class RepositoryBaseClass():
  objectStoreTypeString = None
  objectFactoryFn = None

  @classmethod
  def requireBooleanElement(cls, obj, elementName, objectName):
    cls.RequireStringElement(obj, elementName, objectName)
    if not isinstance(obj[elementName], bool):
      raise RepositoryValidationException(objectName + " must have a " + elementName + " that is a boolean")

  @classmethod
  def reqireIntegerElement(cls, obj, elementName, objectName, minval=None, maxval=None):
    cls.RequireStringElement(obj, elementName, objectName)
    try:
      val = int(obj[elementName])
    except ValueError:
      raise RepositoryValidationException(objectName + " must have a " + elementName + " that is an integer")
    if minval is not None:
      if val < minval:
        raise RepositoryValidationException(objectName + " must have a " + elementName + " that is an integer greater than " + str(minval))
    if maxval is not None:
      if val > maxval:
        raise RepositoryValidationException(objectName + " must have a " + elementName + " that is an integer less than " + str(maxval))

  @classmethod
  def RequireElement(cls, object, elementName, objectName="Object"):
      if elementName not in object:
        raise RepositoryValidationException(objectName + " must have a " + elementName)
      if object[elementName] is None:
        raise RepositoryValidationException(objectName + " must have a " + elementName + " that is not empty")

  @classmethod
  def RequireStringElement(cls, object, elementName, objectName="Object"):
      cls.RequireElement(object, elementName, objectName)
      if object[elementName] == '':
        raise RepositoryValidationException(objectName + " must have a " + elementName + " that is not bank")

  @classmethod
  def RequireListElement(cls, object, elementName, objectName="Object"):
      cls.RequireElement(object, elementName, objectName)
      if not isinstance(object[elementName], list):
        raise RepositoryValidationException(objectName + " must have a " + elementName + " that is a list")

  def __init__(self, objectStoreTypeString, objectFactoryFn=None):
    self.objectStoreTypeString = objectStoreTypeString
    self.objectFactoryFn = objectFactoryFn

  # called before an upsert - should be overridden
  def runUpsertValidation(self, obj):
    pass

  def upsert(self, obj, objectVersion, storeConnection):
    objToStore = copy.deepcopy(obj)
    self.runUpsertValidation(obj)


    if "id" not in objToStore:
      objToStore["id"] = str(uuid.uuid4())
    else:
      if objToStore["id"] is None:
        raise Exception("Invalid object ID")

    newObjVer = storeConnection.saveJSONObject(self.objectStoreTypeString, objToStore["id"], objToStore, objectVersion)

    return objToStore["id"], newObjVer

  def get(self, id, storeConnection):
    obj, objVersion, creationDateTime, lastUpdateDateTime, objKey = storeConnection.getObjectJSON(self.objectStoreTypeString,id)
    if not self.objectFactoryFn is None:
      if obj is None:
        return None
      return self.objectFactoryFn(obj, objVersion, creationDateTime, lastUpdateDateTime, objKey, repositoryObj=self, storeConnection=storeConnection)
    return obj, objVersion, creationDateTime, lastUpdateDateTime, objKey

  def getPaginatedResult(self, paginatedParamValues, outputFN, storeConnection, filterFn=None):
    # Remove sort from all repositories
    paginatedParamValues['sort'] = None
    def internalOutputFunction(item):
      if self.objectFactoryFn is None:
        return outputFN(item)
      return outputFN(self.objectFactoryFn(item[0], item[1], item[2], item[3], item[4], repositoryObj=self, storeConnection=storeConnection))
      #return self.objectFactoryFn(obj, objVersion, creationDateTime, lastUpdateDateTime, objKey)
    def internalFilterFunction(item, searchString):
      if self.objectFactoryFn is None:
        return filterFn(item, searchString)
      return filterFn(self.objectFactoryFn(item[0], item[1], item[2], item[3], item[4], repositoryObj=self, storeConnection=storeConnection), searchString)

    return storeConnection.getPaginatedResult(self.objectStoreTypeString, paginatedParamValues, internalOutputFunction, internalFilterFunction)

  def remove(self, id, storeConnection, objectVersion, ignoreMissingObject = False):
    return storeConnection.removeJSONObject(self.objectStoreTypeString, id, objectVersion=objectVersion, ignoreMissingObject=ignoreMissingObject)

  #Used in devscripts
  def getAllRowsForObjectType(self, filterFN, outputFN, whereClauseText, storeConnection):
    return storeConnection.getAllRowsForObjectType(self.objectStoreTypeString, filterFN, outputFN, whereClauseText)
