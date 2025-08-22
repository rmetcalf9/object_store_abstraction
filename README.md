
# object_store_abstraction

[![Build Status](https://travis-ci.org/rmetcalf9/object_store_abstraction.svg?branch=master)](https://travis-ci.org/rmetcalf9/object_store_abstraction)
[![PyPI version](https://badge.fury.io/py/object_store_abstraction.svg)](https://badge.fury.io/py/object_store_abstraction)


Python library providing abstract object store


# Release process
use coderelease


# installs to make a package:

pip install nose
pip install tox


pip install versioneer
###pip install wheel???

pip install virtualenv
pip install pipenv

## Create pypi file

$HOME/.pypirc
````
[distutils]
index-servers=pypi

[pypi]
repository = https://pypi.python.org/pypi
username = <username>
password = <password>

````

If you leave password blank you will be prompted


## Tpyes

### Object Store
This is the main controlling class that has the settings. It handles transactions.
There are many derived classes for different storage types.


### Repository
This is a repository of objects. 
The main functions (Get/remove/etc.) are called from here
Each has got it's own metadata which carries a object version, save and update times etc.
On creation it is passed a store instance which is ueses.

### Repository Object
This represents an object returned by the repository. This has a save function to update just this record.
It has an "objectStoreTypeString" parameter which segments the data.

### Double String Index
This is equivalent to a repository but much simpler.
There is no object type.
It is just a set of strings which can be quickly looked up. There are no extra metadata stored.
objectStoreTypeString which segments the data.


