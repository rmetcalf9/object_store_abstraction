
# object_store_abstraction

[![Build Status](https://travis-ci.org/rmetcalf9/object_store_abstraction.svg?branch=master)](https://travis-ci.org/rmetcalf9/object_store_abstraction)
[![PyPI version](https://badge.fury.io/py/object_store_abstraction.svg)](https://badge.fury.io/py/object_store_abstraction)


Python library providing abstract object store


# Release process

````
git tag -l #find latest tag


git tag 0.0.1
sudo python3 setup.py sdist
sudo python3 setup.py register sdist upload
git push --tags
````

If you get an error message reporting "dirty" versions can't be uploaded to pypi it means that you have uncommitted changes.


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
