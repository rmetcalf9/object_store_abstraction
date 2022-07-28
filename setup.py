from setuptools import setup
import versioneer

#Dependancy lists maintained here and in tox.ini
sp_install_requires = [
  'pytz==2019.3',
  'sortedcontainers==2.4.0',
  'pyjwt==1.7.1',
  'sqlalchemy==1.3.1',
  'PyMySQL==0.9.3',
  'python-dateutil==2.8.1',
  'boto3==1.24.39'
]
sp_tests_require = [
  'pytest==7.1.27',
  'python_Testing_Utilities==0.1.7'
]

all_require = sp_install_requires + sp_tests_require

setup(name='object_store_abstraction',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Python package which provides an abstract object store',
      url='https://github.com/rmetcalf9/object_store_abstraction',
      author='Robert Metcalf',
      author_email='rmetcalf9@googlemail.com',
      license='MIT',
      packages=['object_store_abstraction', 'object_store_abstraction.objectStoresPackage', 'object_store_abstraction.objectStoresPackage.caching'],
      zip_safe=False,
      install_requires=sp_install_requires,
      tests_require=sp_tests_require,
      include_package_data=True)
