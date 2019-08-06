# -*- coding: utf-8 -*-

"""
    object_store_abstraction
    ~~~~~~~~
    Object store abstraction
    :copyright: (c) 2018 by Robert Metcalf.
    :license: MIT, see LICENSE for more details.
"""

#Exception classes from base
from .objectStores_base import WrongObjectVersionExceptionClass,  SuppliedObjectVersionWhenCreatingExceptionClass, ObjectStoreConfigError,  MissingTransactionContextExceptionClass, UnallowedMutationExceptionClass,  TriedToDeleteMissingObjectExceptionClass, TryingToCreateExistingObjectExceptionClass
#Exception instances from base
from .objectStores_base import WrongObjectVersionException, SuppliedObjectVersionWhenCreatingException, MissingTransactionContextException, UnallowedMutationException, TriedToDeleteMissingObjectException, TryingToCreateExistingObjectException
#Helper fns from base
from .objectStores_base import outputFnJustKeys, outputFnItems

from .objectStores import createObjectStoreInstance, ObjectStoreConfigNotDictObjectExceptionClass, InvalidObjectStoreConfigUnknownTypeClass, InvalidObjectStoreConfigMissingTypeException, InvalidObjectStoreConfigUnknownTypeException, InvalidObjectStoreConfigMissingTypeClass

from .makeDictJSONSerializable import getRJMJSONSerializableDICT, getNormalDICTFromRJMJSONSerializableDICT

#TenantAware exceptions and instances
from .objectStores_TenantAware import CallingNonTenantAwareVersion, CallingNonTenantAwareVersionException, UnsupportedTenantNameException, UnsupportedObjectTypeException

#Allow direct testing of types
from .objectStores_SQLAlchemy import ObjectStore_SQLAlchemy
from .objectStores_Memory import ObjectStore_Memory
from .objectStores_SimpleFileStore import ObjectStore_SimpleFileStore
from .objectStores_DynamoDB import ObjectStore_DynamoDB
from .objectStores_Migrating import ObjectStore_Migrating
from .objectStores_TenantAware import ObjectStore_TenantAware

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
