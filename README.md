# ConveyorBelt
A horizontally scalable headless cluster to shovel Azure diagnostic data (and other custom data) to ElasticSearch

## Introduction
Mixture of Elasticsearch and Kibana is so powerful to analyse and visualise your data - if you have used it once before you know what I mean. Copying small amunt of data to ElasticSearch ES is *trivial* using its document REST API. For moderate amount of data, it can be just the same but would take longer. But for large amount you want to parallelise this operation and use more than one process to do this which requires scheduling. On the other hand, writing the code to transform the data for slightly different schema can be tedious.

These are the exact problems ConveyorBelt tries to solve. Good thing is if you need more nodes, you just add worker instances to your Azure worker role deployment - no more configuration, set up or headache.

## Concepts

Before you start, it would be good to just know a couple of things we keep refer to:

 - **Source**: a date source containing data that needs to be shoveled to Elasticsearch (ES)
 - **Scheduler**: every source requires a scheduler to break down the task of shovelling data from source to smaller chunks so that the actors can pick up. This scheduler is normally a class implementation of `ISourceScheduler` interface
 - **Mapping**: "Schema" for ES data. Every index can have different "type"s and each type is denoted by its mapping (i.e. its schema). A mapping defines list of fields, theit data types and how each field should be indexed.
 - **BeeHive**: A Reactive Actor mini-Framework used in the implementation of ConveyorBelt
 - **LastOffsetPoint**: A pointer to the last scheduled position in the source. Commonly this position is a timestamp. 

## Getting started (shovel your first data source to Elasticsearch)
OK, I guess you have some Azure diagnostic logs in `WADLogsTable` table that you would like to transfer to Elasticsearch (ES). To set this up, you need:

 1. An Azure Subscription to run a worker role instance of ConveyorBelt
 2. An Azure Storage Account to store small amount of data required for configuration and some state
 3. An Azure Service Service Bus namespace for work queues
 4. Obviously, an Elasticsearch server

You can also run this on your machine using Azure Emulator and [Service Bus for Windows Server](https://msdn.microsoft.com/en-us/library/dn282144.aspx): just clone the repo, install Service Bus for Windows, set up configuration parameters and click F5! But the rest of this intro is about running on Azure.

### Step 1: Create configuration
Go to the build forlder and copy and rename `tokens.json.template` file to `tokens.json`. Then edit the file and set up these paraneters:

 - `ConveyorBelt_ElasticSearchUrl`: URL to the server such as http://myserver:9200/ (end it with /)
 - `ConveyorBelt_TableName`: Name of the sources table. `DiagnosticsSource` is just fine.
 - `ConveyorBelt_ServiceBus_ConnectionString`: Connection string to the service bus. It should have read-write-manage access.
 - `ConveyorBelt_MappingsPath`: A root path of the URL containing mappings. Typically a public read-only Azure Blob location that contains the mapping. You can find common mappings in the `mappings` folder of this repository.
 - `ConveyorBelt_Storage_ConnectionString`: Connection string to the storage location where your table is located at. This storage will be used for cluster's lock management
 - `ConveyorBelt_Diagnostics_ConnectionString`: Worker role's diagnostic storage connection string

### Step 2: Build and deploy
Now open a powershell window, add your subscription and accounts and then run `PublishCloudService.ps1` with these parameters:

```PowerShell
.\PublishCloudService.ps1 `
  -serviceName <name your ConveyorBelt Azure service> `
  -storageAccountName <name of the storage account for the service> `
  -subscriptionDataFile <your .publishsettings file> `
  -selectedsubscription <subscription to use> `
  -affinityGroupName <affinity group or Azure region to deploy to>
```

Now you should have a running cluster of ConveyorBelt!

### Step 3: Set up your source
In Azure Storage Explorer (or your favourite tool) open `DiagnosticsSource` table (or whatever you named it) and create an item with properties described below:

 - PartitionKey: whatever you like - commonly <top level business domain>_<mid level business domain> 
 - RowKey: whatever you like - commonly <env: live/test/integration>_<service name>_<log type: logs/wlogs/perf/iis/custom>
 - ConnectionString (string): connection string to the Storage Account containing `WADLogsTable`
 - GracePeriodMinutes (int): Depends on how often your logs gets copied to Azure table. If it is 10 minutes then 15 should be ok, if it is 1 minute then 3 is fine.
 - IsActive (bool): True
 - MappingName (string): `WADLogsTable`. ConveyorBelt would look for mapping in <ConveyorBelt_MappingsPath>/<MappingName>.json
 - LastOffsetPoint (string): set to ISO Date (second and millisecond must be zero) from which you want the data to be copied e.g. 2015-02-15T19:34:00.0000000+00:00
 - LastScheduled (datetime): set it to a date in the past
 - MaxItemsInAScheduleRun (int): 100000 is fine
 - SchedulerType (string): ConveyorBelt.Tooling.Scheduling.MinuteTableShardScheduler
 - SchedulingFrequencyMinutes (int): 1
 - TableName (string): WADLogsTable

That is it! After a few minutes, you should see documents appearing in your ES and after a while, all data since the initial LastOffsetPoint transfered to elasticsearch.


