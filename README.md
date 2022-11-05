
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

# Run tests
````
nosetests
````

````
pipenv shell
pipenv run tox
````

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
