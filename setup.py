from setuptools import setup
import versioneer

#Dependancy lists maintained here and in tox.ini
sp_install_requires = [
  'pytz==2025.2',
  'sortedcontainers==2.4.0',
  'pyjwt==2.8.0',
  'sqlalchemy==2.0.44',
  'PyMySQL==0.9.3',
  'python-dateutil==2.9.0.post0',
  'boto3==1.40.75'
]
sp_tests_require = [
  'pytest==8.1.0',
  'python_Testing_Utilities==0.1.11'
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
      packages=[
          'object_store_abstraction',
          'object_store_abstraction.objectStoresPackage',
          'object_store_abstraction.objectStoresPackage.caching',
          'object_store_abstraction.DoubleStringIndex'
      ],
      zip_safe=False,
      install_requires=sp_install_requires,
      tests_require=sp_tests_require,
      include_package_data=True)
