# Introduction
Here is a Python code which is ingesting data from a MS SQL database into the Azure Data Lake. We are performing here both full truncate and load, and incremental load.

In this repository we are using the Delta Lake framework for storing data as delta tables in Azure Data Lake. We are using the deltalake library ([delta-io.github.io/delta-rs](https://delta-io.github.io/delta-rs/)). Don't confuse it with the delta library ([docs.delta.io/0.4.0/api/python](https://docs.delta.io/0.4.0/api/python/index.html#), [docs.delta.io/latest](https://docs.delta.io/latest/index.html)). Delta library requires to use Spark engine, deltalake doesn't.

In full truncate and load we are truncating the entire target table in Data Lake and loading into it the entire data from the source table in the SQL db.

In the incremental load we are only updating the target table based on changes which happened to the source table. For that we are using a changes table which describes what changes happened to the source table, that is which records have been modified, deleted and inserted.

This code creates a container in Data Lake into which we will be ingesting data.

It also creates another container with a delta table that contains extract logs. They contain information about when the last time we were extracting data for each table what is needed for incremental load. For working with extract logs we are using the ExtractLog class.

All the scripts mentioned in this documentation are contained in the data_processing folder.

# How to use those scripts
We can use those scripts in order to simulate an incremental load from a MS SQL db into a Azure Data Lake. In order to do that we need to follow the below steps. All those steps and scripts used in them are described below in this document in the sections 'SQL db data ingestion' and 'Data Lake data ingestion'.
- **SQL db data ingestion part 1** - First we are preparing initial data in the SQL db which will be later on ingested into the Data Lake. We do this using the sql_ingestion_v1.py script. We are preparing here 3 tables:
    - db.fact.table1 - data from this table will be ingested into the Data Lake using a full truncate and load.
    - db.fact.table2 - data from this table will be ingested into the Data Lake using an incremental load.
    - db.fact.table2_changes - changes table describing what changes happened to the 'table2' table. It will be used for an incremental load.
- **Data Lake setup** - After that we need to create a container and directory in that container where we will be ingesting data. For that we are using the data_lake_setup.py script which is using Azure SDK.
- **Data Lake data ingestion part 1** - Then we can ingest data from SQL db which we prepared in the previous step into the Data Lake. We do this using the data_lake_ingestion.py script. We are performing full truncate and load for db.fact.table1 and incremental load for db.fact.table2
- **SQL db data ingestion part 2** - As the third step we are making changes in the 'table2' and 'table2_changes' tables in the SQL db:
    - We are ingesting one additional record into the 'table2' table.
    - We are modifying one record into the 'table2' table.
    - We are deleting one record into the 'table2' table.
    - We save information about those changes in the table2_changes table.
    We make those changes in order to test if incremental load is working properly. We perform this step using the sql_ingestion_v2.py script.
- **Data Lake data ingestion part 2** - In the end we are performing again data ingestion into the Data Lake in order to test incremental load. We should add one record to the target table, modify one and delete one. We do this using again the data_lake_ingestion.py script.

# Prerequisites
Before we use that code we need to perform the following preparations:
- **Create an Azure SQL database and Azure Data Lake** - between which we will be transferring data. For that we can use the azure_terraform repository, data_lake and sql_db modules.
- **Set up firewall rules in the SQL database** - in order to connect to the created SQL database we need to add our IP address to the firewall rules. We can do that in Azure platform if we go to the MS SQL server resource > security > networking. There we can add the IP address of our currently used computer to the firewall rules.
- **Save credentials in the .env file** - In the .env file we need to save credentials for all the variables which we are accessing in the code using the os.getenv() function. Those are:
    - ACCOUNT_NAME - Name of the Storage Account (Data Lake)
    - ACCESS_KEY - Data Lake access key. When we are creating a Data Lake using the data_lake module from the azure_terraform repository, the access key is saved in the Terraform output 'primary_access_key'.
    - SQL_SERVER_NAME - Name of the SQL server from which we will be ingesting data.
    - SQL_DB_NAME - Name of the db in the SQL server from which we will be ingesting data (it can be any database on that server).
    - SQL_USERNAME - username used for logging into the SQL server.
    - SQL_PASSWORD - password used for logging into the SQL server.

# Script configuration
In the config.py file we are assigning values to variables defining names of the container and directory in that container where we will be ingesting data in Data Lake.

# SQL db data ingestion
We are ingesting data into the SQL db using the sql_ingestion_v1.py and sql_ingestion_v2.py scripts. At the begining of those scripts we are defining what records are gonna be ingested into which tables. Data from those tables will be ingested into the Data Lake as described in the section above 'How to use those scripts'.

Into the changes table 'table2_changes' we are ingesting the current date into the column indicating when that record was created (that is when the change happened to the source table 'table2').

# Data Lake data ingestion
The data_lake_ingestion.py script is ingesting data from SQL db into the Data Lake. Before we do that we need to at first ingest data into that SQL db what is described in the section above 'How to use those scripts'.

That script creates the 'source-data' container in that Data Lake and 'source_data' folder in that container where we will be saving data ingested from the SQL db. It also creates the 'extract-logs' container with the 'extract_logs' delta table containing extract logs. For creating containers and folders it is using the AzureBlob class which is using Azure SDK.

At the begining of that script we need to provide information about:
- From which tables we will be ingesting data and provide additional information needed to perform an incremental load.
- Name of the container and directory which we will create and where we will be ingesting data.

# Data Lake cleanup
The data_lake_cleanup.py script deletes: 
- **Ingested data from SQL** - It deletes the source_data directory in the source-data container where we were ingesting data from SQL db.
- **Extract logs** - It deletes the extract_logs directory from the extract-logs container containing extract logs (when the last time we extracted data for each table).

# Classes
We have the following classes defined:
- **AzureBlob** - This is a class for working with containers, directories and files (creating them, deleting, renaming). This class is a parent to the DeltaLake and ExtractLogs classes. It is using Azure SDK.
- **DeltaLake** - This is a class for working with delta tables (creating, writing data, reading data, updating them incrementally). It extends the AzureBlob class.
- **SQL** - This is a class for working with SQL. This class is used in the DataIngestion class.
- **ExtractLog** - This is a class for working with extract logs which are used for incremental load. This class is a parent to the DataIngestion class.
- **DataIngestion** - This is a class for creating data ingestion pipelines.