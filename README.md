# Custom log aggregation and processing with Kafka, Flink and Solr

## Running the Flink application

Before we can start our Flink application, we must create the Solr collection that will be populated with the logs. We can simply do this in 2 steps using the command line client:

```
solrctl config --create flink-logs-conf schemalessTemplate -p immutable=false
solrctl collection --create flink-logs -c flink-logs-conf
```

In a secure environment we need to run `kinit` before these commands.

Once the collection is ready we can create out job properties file:
```
# General props
log.input.topic=flink.logs
log.indexing.batch.size.seconds=10

# Solr props
solr.urls=<solr-host:port>/solr
solr.collection=flink-logs
solr.ssl.truststore.location=/samePathOnAllNodes/truststore.jks

# Kafka props
kafka.group.id=flink
kafka.bootstrap.servers=<kafka-brokers>

kafka.security.protocol=SASL_SSL
kafka.sasl.kerberos.service.name=kafka
kafka.ssl.truststore.location=/samePathOnAllNodes/truststore.jks
```

**Run command**

Unsecure environment

```
flink run -m yarn-cluster -p 2 -ytm 2000 job.jar --properties.file job.props
```

Secure environment

```
flink run -m yarn-cluster -p 2 -ytm 2000 -yt kafka.jaas.conf -yD security.kerberos.login.keytab=test.keytab -yD security.kerberos.login.principal=test job.jar --properties.file job.props
```
