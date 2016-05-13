# jmxtrans-output-elastic-aggregate

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
