# Datastore type option

This page explains the options available for different datastore types.

# Memory Datastore

Paramater passed as objectStoreConfigDict to createObjectStoreInstance
```
{
  "Type": "Memory"
}
```

or sample enviroment variable setup
```
export APIAPP_OBJECTSTORECONFIG="{\"Type\":\"Memory\"}"
```

# SQLAlchemy datastore

Paramater passed as objectStoreConfigDict to createObjectStoreInstance
```
{
  "Type": "SQLAlchemy",
  "connectionString": "mysql+pymysql://saas_user_man_user:saas_user_man_testing_password@127.0.0.1:10103/saas_user_man_rad"
}
```
Add "objectPrefix" to prefix tables used.

or sample environment variable setup
```
export APIAPP_OBJECTSTORECONFIG="{\"Type\":\"SQLAlchemy\", \"connectionString\":\"mysql+pymysql://saas_user_man_user:saas_user_man_testing_password@127.0.0.1:10103/saas_user_man_rad\"}"
```

Note: command line to connect:
```
mysql -h 127.0.0.1 -P 10103 -u saas_user_man_user -p saas_user_man
```


# Simple file system datastore

Paramater passed as objectStoreConfigDict to createObjectStoreInstance
```
{
  "Type": "SimpleFileStore",
  "BaseLocation": "/var/datastore"
}
```
Base location has no trailing slash

# DynamoDB datastore

This module creates sessions as per https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

```
{
  "Type": "DynamoDB",
  "aws_access_key_id": "ACCESS_KEY",
  "aws_secret_access_key": "SECRET_KEY",
  "region_name": "eu-west-2",
  "endpoint_url": "http://localhost:8000",
  "single_table_mode": "True"
}
```
(eu-west-2 = London)

Endpoint URL is used when testing with a local endpoint, if not the url to send it should be the text value "None"

Access keys can be obtained following instructions here https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SettingUp.DynamoWebService.html#SettingUp.DynamoWebService.GetCredentials

Add "objectPrefix" to prefix tables used.

single_table_mode should probably be set as there are only 256 tables per AWS account

# Migrating Datastore

This is a special datastore type which allows data to be live migrated from one data store to another. Two stores are specified and both are connected to. All data reads are produced from the "From" store. All save and remove operations are propagated to both "From" and "To" data stores. This means the following process can be used in doing a migration:

1. To start with the micro-service is running connected to data store A
2. New empty data store B is setup
3. New version of micro-service is deployed in migrating mode with A set as the From store and B set as the to store.
4. While the new version is running a one off migration job is run which migrates all data. Due to the fact that the system is running in Migrating mode when this job completes data store B is always kept with the most up to date version of the dataset.
5. A second new version of the micro-service is deployed connected to data store B

```
{
  "Type": "Migrating",
  "From": {**},
  "To": {**}
}
```

## Migration job

Depending on the datastores used, the amount of data, and other considerations different methods to migrate the data can be used.

 - Migration via app that uses object store - if it has a function to load and resave all records this should effect the migration.
 - Manual extract of data from the origin store and upload to the target.
 - Run utility in samples which will preforma a migration. This is a little brittle as it loads all object keys into memory before doing the migration so will not work on larger data sizes.
 - Develop passive migration program which stays online for a while and migrates records one by one in a throttled manor.

# Caching Datastore

```
{
  "Type": "Caching",
  "Caching": {**}, #If missing memory is used
  "Main": {**}, #Main persistant store
  "DefaultPolicy": {
    "cache": True/False,
    "maxCacheSize": 100,
    "cullToSize": 50,
    "timeout": 60 000  #Miliseconds (Example is 1 minute)
  },
  "TablePolicyOverride": {
    "**TableNAme**": {**} #Same as DefaultPolicy
  }
}
```

## Timing uses time.pref_counter
time.perf_counter()
See https://realpython.com/python-timer/

