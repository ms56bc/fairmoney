A  application project using Scala and maven.

## How to run
1. Start Cluster. This will build the project and start the cluster, create topics, tables and submit jobs to flink.
```shell
  make start
```

### The Service architecture is as follows:
```el

                        +-------------------------------------+
            ----------->|           Schema Registry           |---------
           |            +-------------------------------------+         |
           |                                                            |  
           |                                                            v
+-------------------+        +----------------------+        +---------------------+        +-----------------+
|   Producer        |        |        Kafka         |        |   ML Feature Job    |        |    SCYLLA  Sink |
|                   |        |                      |        |    Flink            |        |   KV Store      |
| [Data Generation]--+--------> [Kafka Topic]         +--------> [Processing Logic] +--------> [SCYLLA Table] |
|                   |        |                      |        |                     |        |                 |
+-------------------+        +----------------------+        +---------------------+        +-----------------+
                                   |                                         |
                                   v                                         v
                            +------------------------+              +--------------------+
                            |  Flink  Backup JOB     |              | Flink Sink         |            
                            | Avro File              |              | User stats in avro |
                            | Local File system      |              | file               |    
                            +------------------------+              +--------------------+

```
### Architecture Choices:
- **Kafka**: Kafka cluster is used as a asynchronous message processing system.
- **SCYLLA**: This is used as a key value store to persist the user_transaction features.
- **Local File system**: Local files system is used to store the avro files for both the backup and user_stats, 
  however the system could easily be modified to use any distributed file system like s3.
- **Avro File**: Mainly for their schema evolution capabilities and support for schema registry.
- **Flink**: Flink is used to process the data from kafka topic and generate the features. It can also scale to handle large volumes of data.

### The Application has three modules and other folders:
- **TransactionProducer**: This is a data generation application which generates data and publishes to a kafka topic transactions.
- **MLFeatureJob**: This is a spark application which reads the data from kafka topic and processes it to generate user transaction count.
    The count is generated every minute and is stored in a scylla table.
- **BackupJob**: This is a spark application which reads the data from kafka topic and processes it to generate features.
- **data**: This folder is mounted to the flink cluster and will store all the file generated like backup and user stats.
- **scripts**: This folder contains the scripts to create-table and submit job to flink.

### The Historical data backfill:
- The historical data backfill is done by running the MLFeatureJob job input arguments --backfill and --backfillpath. 
  This will make an alternative path of stream execution configuring the flink job to read from the backup files and then write to files skipping the key-value sink.

**Functionality Not Covered**:
- The application is not configured to use users to connect with schema registry.
- Lot could be added to tests but due to time constraints only basic tests are added.
- Logging is missing.
- Metrics are not configured.


