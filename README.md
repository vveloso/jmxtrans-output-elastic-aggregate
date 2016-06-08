# jmxtrans-output-elastic-aggregate

[![Build Status](https://travis-ci.org/vveloso/jmxtrans-output-elastic-aggregate.svg?branch=master)](https://travis-ci.org/vveloso/jmxtrans-output-elastic-aggregate)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.vveloso/jmxtrans-output-elastic-aggregate/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.vveloso/jmxtrans-output-elastic-aggregate)

JMXTrans output writer for Elasticsearch which outputs one structured document for each query.

Documents pushed to Elastic have two automatically generated attributes, `@metadata` and `@timestamp`. An example of metrics obtained for a JMS queue would be:

```
{
  "@timestamp": 1463071268789,
  "@metadata": {
	"typeName": "module=Core,type=Queue,address=\"jms.queue.nb8Queue\",name=\"jms.queue.nb8Queue\"",
	"server": "server.example.com",
	"typeNameValues": "Queue_\"jms.queue.nb8Queue\"_\"jms.queue.nb8Queue\"",
	"port": "9990"
  },
  "MessagesAdded": 0,
  "Temporary": false,
  "ConsumerCount": 20,
  "DeliveringCount": 0,
  "Durable": false,
  "MessageCount": 0,
  "ScheduledCount": 0
}
```

Sample configuration:

```
{
  "servers" : [ {
	"url" : "service:jmx:http-remoting-jmx://server.example.com:9990",
    "outputWriters" : [ {
	  "@class" : "com.googlecode.jmxtrans.model.output.elastic.ElasticAggregateWriter",
	  "elasticHostName" : "elastic.example.com",
	  "booleanAsNumber" : false,
	  "elasticTypeName" : "jmx-entry",
	  "elasticIndexName" : "tnms_jmx-entries",
	  "typeNames" : [ "type", "address", "name", "data-source", "service" ]
	}	],
    "queries" : [ {
      "obj" : "java.lang:type=Memory",
      "attr" : [ "HeapMemoryUsage", "NonHeapMemoryUsage" ]
    }, {
	  "obj" : "org.hornetq:module=Core,type=Queue,address=*,name=*",
      "attr" : [ "ConsumerCount", "DeliveringCount", "Durable", "MessageCount", "MessagesAdded", "ScheduledCount", "Temporary" ]
	} ]
  } ]
}
```

## Index partitioning

It is possible to use `Formatter` date time conversion suffix characters in index names. See [java.util.Formatter Date/Time Conversions](https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html#dt) for details on formatting.

For example, the configuration above could be modified to support indexes partitioned daily by using `"elasticIndexName" : "jmx-entries-%1$tY-%1$tm-%1$td"` instead.
