# Custom log aggregation and processing with Kafka, Flink and Solr

## Build

To build the job artifacts simply run:

```
mvn clean package
```

This will create a jar named `flink-solr-log-indexer-1.0-SNAPSHOT.jar` in the `target` folder that you will use to start the application.

## Running the Flink application

Before we can start our Flink application, we must create the Solr collection that will be populated with the logs. We can simply do this in 2 steps using the command line client:

```
solrctl config --create flink-logs-conf schemalessTemplate -p immutable=false
solrctl collection --create flink-logs -c flink-logs-conf
```

In a secure environment we need to run `kinit` before these commands.

Once the collection is ready we can create out job properties file (`solr_indexer.props`):
```
# General props
log.input.topic=flink.logs
log.indexing.batch.size.seconds=10

# Solr props
solr.urls=<solr-host:port>/solr
solr.collection=flink-logs

# Kafka props
kafka.group.id=flink
kafka.bootstrap.servers=<kafka-brokers>
```

**Run command**

We will use the good old Flink CLI to submit our job to the cluster (YARN in this example):

```
flink run -m yarn-cluster -p 2 flink-solr-log-indexer-1.0-SNAPSHOT.jar --properties.file solr_indexer.props
```

Feel free to increase the parallelism and other resources of the job to scale up to your logging rate.

## Secure environment

In a secure environment we need to specify a few extra properties for Solr and Kafka in our `solr_indexer.props` file:

```
solr.ssl.truststore.location=/samePathOnAllNodes/truststore.jks

kafka.security.protocol=SASL_SSL
kafka.sasl.kerberos.service.name=kafka
kafka.ssl.truststore.location=/samePathOnAllNodes/truststore.jks
```

We also need to specify the Kerberos settings in our flink configuration:

```
security.kerberos.login.keytab=test.keytab
security.kerberos.login.principal=test
```
