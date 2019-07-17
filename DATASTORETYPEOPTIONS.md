# Datastore type option

This page explains the options availiable for different datastore types.

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
  "endpoint_url": "http://localhost:8000"
}
```
(eu-west-2 = London)

Endpoint URL is used when testing with a local endpoint, if not the url to send it should be the text value "None"

Access keys can be obtained following instructions here https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SettingUp.DynamoWebService.html#SettingUp.DynamoWebService.GetCredentials

Add "objectPrefix" to prefix tables used.
