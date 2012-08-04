ActiveMQ River Plugin for ElasticSearch
==================================
The ActiveMQ River plugin allows index bulk format messages into elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install domdorn/elasticsearch-river-activemq/1.0`.


The ActiveMQ River allows to automatically index a [ActiveMQ](http://activemq.apache.org/) queue. The format of the messages follows the bulk api format:

	{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "this is a tweet" } }
	{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
	{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "another tweet" } }    


Creating the ActiveMQ river is as simple as (all configuration parameters are provided, with default values):

```bash
  curl -XPUT 'localhost:9200/_river/name_of_your_river/_meta' -d '{
      "type" : "activemq",
      "activemq" : {
          "user" : "guest",
          "pass" : "guest",
          "brokerUrl" : "failover://tcp://localhost:61616",
          "sourceType" : "queue",
          "sourceName" : "elasticsearch",
          "consumerName" : "activemq_elasticsearch_river_" + rivername,
          "durable" : false
          "filter" : ""
      },
      "index" : {
           "bulk_size" : 100,
           "bulk_timeout" : "10ms"
      }
  }'
```

Valid values for the sourceType property are either "queue" or "topic".
The durable and filter options are only available for topics.

To delete the river at a later state (e.g. if you made some configuration mistake), simply do:
```bash
  curl -XDELETE 'localhost:9200/_river/name_of_your_river'
```


To get the status of the river infrastructure of elasticsearch itself, you can do:
```bash
  curl -XGET 'localhost:9200/_river/_status'
```

This ElasticSearch River for the ActiveMQ Message Queue is based on the excellent work
of the RabbitMQ ElasticSearch River. 

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2012 ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.

