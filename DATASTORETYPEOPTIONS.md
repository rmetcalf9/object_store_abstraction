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

or sample enviroment variable setup
```
export APIAPP_OBJECTSTORECONFIG="{\"Type\":\"SQLAlchemy\", \"connectionString\":\"mysql+pymysql://saas_user_man_user:saas_user_man_testing_password@127.0.0.1:10103/saas_user_man_rad\"}"
```
