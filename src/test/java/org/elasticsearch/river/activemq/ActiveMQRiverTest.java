/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.*;
import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author Dominik Dorn // http://dominikdorn.com
 */
public class ActiveMQRiverTest {

    final String message = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
            "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
            "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
            "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
            "{ \"type1\" : { \"field1\" : \"value1\" } }";

    final String message2 = "{ \"index\" : { \"_index\" : \"test2\", \"_type\" : \"type2\", \"_id\" : \"1\" }\n" +
            "{ \"type2\" : { \"field2\" : \"value2\" } }\n" +
            "{ \"delete\" : { \"_index\" : \"test2\", \"_type\" : \"type2\", \"_id\" : \"2\" } }\n" +
            "{ \"create\" : { \"_index\" : \"test2\", \"_type\" : \"type2\", \"_id\" : \"1\" }\n" +
            "{ \"type2\" : { \"field2\" : \"value2\" } }";

    BrokerService broker;
    Client client;

    private void startActiveMQBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:61616");
        broker.start();
    }

    private void stopActiveMQBroker() throws Exception {
        for(TransportConnector c : broker.getTransportConnectors())
        {
            try{
                c.stop();
                broker.removeConnector(c);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        broker.getBroker().stop();
    }

    private void startElasticSearchDefaultInstance() throws IOException {
        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();
        client = node.client();
        client.prepareIndex("_river", "test1", "_meta").setSource(jsonBuilder().startObject().field("type", "activemq").endObject()).execute().actionGet();
    }

    private void startElasticSearchSourceNameInstance(String sourceName) throws IOException {
        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();
        client = node.client();
        client.prepareIndex("_river", "test1", "_meta").setSource(jsonBuilder().startObject()
                .field("type", "activemq")
                .field("sourceName", sourceName)
                .endObject()).execute().actionGet();
    }

    private void stopElasticSearchInstance() {
        System.out.println("shutting down elasticsearch");
        client.admin().cluster().prepareNodesShutdown().execute();
        client.close();
    }


    @Test
    public void testSimpleScenario() throws Exception {

        startActiveMQBroker();
        startElasticSearchDefaultInstance();


        // assure that the index is not yet there
        try {
            ListenableActionFuture future = client.prepareGet("test", "type1", "1").execute();
            future.actionGet();
            Assert.fail();
        } catch (IndexMissingException idxExcp) {

        }


        // connect to the ActiveMQ Broker and publish a message into the default queue
        postMessageToQueue(ActiveMQRiver.defaultActiveMQSourceName, message);


        Thread.sleep(3000l);

        {
            ListenableActionFuture future = client.prepareGet("test", "type1", "1").execute();
            Object o = future.actionGet();
            GetResponse resp = (GetResponse) o;
            Assert.assertEquals("{ \"type1\" : { \"field1\" : \"value1\" } }", resp.getSourceAsString());
        }


        stopElasticSearchInstance();
        stopActiveMQBroker();

        Thread.sleep(3000l);

    }

    @Test
    public void testRiverWithNonDefaultName() throws Exception {

        String riverCustomSourceName = "nonDefaultNameQueue";

        startActiveMQBroker();
        startElasticSearchSourceNameInstance(riverCustomSourceName);


        // assure that the index is not yet there
        try {
            ListenableActionFuture future = client.prepareGet("test2", "type2", "2").execute();
            future.actionGet();
            Assert.fail();
        } catch (IndexMissingException idxExcp) {

        }

        postMessageToQueue(riverCustomSourceName, message2);


        Thread.sleep(3000l);

        {
            ListenableActionFuture future = client.prepareGet("test2", "type2", "2").execute();
            Object o = future.actionGet();
            GetResponse resp = (GetResponse) o;
            Assert.assertEquals("{ \"type2\" : { \"field2\" : \"value2\" } }", resp.getSourceAsString());
        }


        stopElasticSearchInstance();
        stopActiveMQBroker();

        Thread.sleep(3000l);

    }

    private void postMessageToQueue(final String sourceName, final String msgText) {
        // connect to the ActiveMQ Broker and publish a message into the default queue
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        try {
            Connection conn = factory.createConnection();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(sourceName);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(msgText));

            session.close();
            conn.close();
        } catch (JMSException e) {
            Assert.fail("JMS Exception");
        }
    }


}
