#Test object stores creation
from TestHelperSuperClass import testHelperSuperClass
import object_store_abstraction as undertest
import json
import os

'''
#Sample code used to create a store in an application:
    objectStoreConfigJSON = readFromEnviroment(env, 'APIAPP_OBJECTSTORECONFIG', '{}', None)
    objectStoreConfigDict = None
    try:
      if objectStoreConfigJSON != '{}':
        objectStoreConfigDict = json.loads(objectStoreConfigJSON)
    except Exception as err:
      print(err) # for the repr
      print(str(err)) # for just the message
      print(err.args) # the arguments that the exception has been called with.
      raise(InvalidObjectStoreConfigInvalidJSONException)

    fns = {
      'getCurDateTime': self.getCurDateTime,
      'getPaginatedResult': self.getPaginatedResult
    }
    self.objectStore = createObjectStoreInstance(objectStoreConfigDict, fns)
'''

SKIPSQLALCHEMYTESTS=False
if ('SKIPSQLALCHEMYTESTS' in os.environ):
  if os.environ["SKIPSQLALCHEMYTESTS"]=="Y":
    SKIPSQLALCHEMYTESTS=True


#@TestHelperSuperClass.wipd
class test_objectStoresMemory(testHelperSuperClass):
  def test_defaultCreation(self):
    objectStoreConfigDict = None
    a = undertest.createObjectStoreInstance(objectStoreConfigDict, self.getObjectStoreExternalFns())
    if not isinstance(a,undertest.ObjectStore_Memory):
      self.assertTrue(False,msg='Wrong type of object store created')

  def test_memoryCreation(self):
    a = "{\"Type\":\"Memory\"}"
    objectStoreConfigDict = json.loads(a)
    a = undertest.createObjectStoreInstance(objectStoreConfigDict, self.getObjectStoreExternalFns())
    if not isinstance(a,undertest.ObjectStore_Memory):
      self.assertTrue(False,msg='Wrong type of object store created')

  def test_sqlAlchemyCreation(self):
    if SKIPSQLALCHEMYTESTS:
      print("Skipping SQLAlchemyTests")
      return
    a = "{\"Type\":\"SQLAlchemy\", \"connectionString\":\"mysql+pymysql://saas_user_man_user:saas_user_man_testing_password@127.0.0.1:10103/saas_user_man_rad\"}"
    objectStoreConfigDict = json.loads(a)
    a = undertest.createObjectStoreInstance(objectStoreConfigDict, self.getObjectStoreExternalFns())
    if not isinstance(a,undertest.ObjectStore_SQLAlchemy):
      self.assertTrue(False,msg='Wrong type of object store created')

  def test_nonDictPassedToCreation(self):
    with self.assertRaises(Exception) as context:
      a = undertest.createObjectStoreInstance("Not A Dict", self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.ObjectStoreConfigNotDictObjectExceptionClass)

  def test_dictWithoutTypePassedToCreation(self):
    objectStoreConfigDict = {'Som': 'dsds'}
    with self.assertRaises(Exception) as context:
      a = undertest.createObjectStoreInstance(objectStoreConfigDict, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.InvalidObjectStoreConfigMissingTypeClass)

  def test_dictWithUnknownTypePassedToCreation(self):
    objectStoreConfigDict = {'Type': 'SomeInvalidObjectStoreType'}
    with self.assertRaises(Exception) as context:
      a = undertest.createObjectStoreInstance(objectStoreConfigDict, self.getObjectStoreExternalFns())
    self.checkGotRightExceptionType(context,undertest.InvalidObjectStoreConfigUnknownTypeClass)
