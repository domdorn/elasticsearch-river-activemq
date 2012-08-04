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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import javax.jms.*;
import java.util.List;
import java.util.Map;

/**
 * &quot;Native&quot; ActiveMQ River for ElasticSearch.
 *
 * @Author Dominik Dorn || http://dominikdorn.com
 * based on the work of the ElasticSearch RabbitMQ River team
 */
public class ActiveMQRiver extends AbstractRiverComponent implements River {

    public final String defaultActiveMQUser = ActiveMQConnection.DEFAULT_USER;
    public final String defaultActiveMQPassword = ActiveMQConnection.DEFAULT_PASSWORD;
    public final String defaultActiveMQBrokerUrl = ActiveMQConnection.DEFAULT_BROKER_URL;
    public static final String defaultActiveMQSourceType = "queue"; // topic
    public static final String defaultActiveMQSourceName = "elasticsearch";
    public final String defaultActiveMQConsumerName;
    public final boolean defaultActiveMQCreateDurableConsumer = false;
    public final String defaultActiveMQTopicFilterExpression = "";

    private String activeMQUser;
    private String activeMQPassword;
    private String activeMQBrokerUrl;
    private String activeMQSourceType;
    private String activeMQSourceName;
    private String activeMQConsumerName;
    private boolean activeMQCreateDurableConsumer;
    private String activeMQTopicFilterExpression;


    private final Client client;

    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    private volatile boolean closed = false;

    private volatile Thread thread;

    private volatile ConnectionFactory connectionFactory;

    @SuppressWarnings({"unchecked"})
    @Inject
    public ActiveMQRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;
        this.defaultActiveMQConsumerName = "activemq_elasticsearch_river_" + riverName();

        if (settings.settings().containsKey("activemq")) {
            Map<String, Object> activeMQSettings = (Map<String, Object>) settings.settings().get("activemq");
            activeMQUser = XContentMapValues.nodeStringValue(activeMQSettings.get("user"), defaultActiveMQUser);
            activeMQPassword = XContentMapValues.nodeStringValue(activeMQSettings.get("pass"), defaultActiveMQPassword);
            activeMQBrokerUrl = XContentMapValues.nodeStringValue(activeMQSettings.get("brokerUrl"), defaultActiveMQBrokerUrl);
            activeMQSourceType = XContentMapValues.nodeStringValue(activeMQSettings.get("sourceType"), defaultActiveMQSourceType);
            activeMQSourceType = activeMQSourceType.toLowerCase();
            if (!"queue".equals(activeMQSourceType) && !"topic".equals(activeMQSourceType))
                throw new IllegalArgumentException("Specified an invalid source type for the ActiveMQ River. Please specify either 'queue' or 'topic'");
            activeMQSourceName = XContentMapValues.nodeStringValue(activeMQSettings.get("sourceName"), defaultActiveMQSourceName);
            activeMQConsumerName = XContentMapValues.nodeStringValue(activeMQSettings.get("consumerName"), defaultActiveMQConsumerName);
            activeMQCreateDurableConsumer = XContentMapValues.nodeBooleanValue(activeMQSettings.get("durable"), defaultActiveMQCreateDurableConsumer);
            activeMQTopicFilterExpression = XContentMapValues.nodeStringValue(activeMQSettings.get("filter"), defaultActiveMQTopicFilterExpression);

        } else {
            activeMQUser = (defaultActiveMQUser);
            activeMQPassword = (defaultActiveMQPassword);
            activeMQBrokerUrl = (defaultActiveMQBrokerUrl);
            activeMQSourceType = (defaultActiveMQSourceType);
            activeMQSourceName = (defaultActiveMQSourceName);
            activeMQConsumerName = defaultActiveMQConsumerName;
            activeMQCreateDurableConsumer = defaultActiveMQCreateDurableConsumer;
            activeMQTopicFilterExpression = defaultActiveMQTopicFilterExpression;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
        } else {
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            ordered = false;
        }
    }


    @Override
    public void start() {
        logger.info("Creating an ActiveMQ river: user [{}], broker [{}], sourceType [{}], sourceName [{}]",
                activeMQUser,
                activeMQBrokerUrl,
                activeMQSourceType,
                activeMQSourceName
        );
        connectionFactory = new ActiveMQConnectionFactory(activeMQUser, activeMQPassword, defaultActiveMQBrokerUrl);

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "activemq_river").newThread(new Consumer());
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("Closing the ActiveMQ river");
        closed = true;
        thread.interrupt();
    }

    private class Consumer implements Runnable {

        private Connection connection;

        private Session session;
        private Destination destination;

        @Override
        public void run() {
            while (true) {
                if (closed) {
                    break;
                }
                try {
                    connection = connectionFactory.createConnection();
                    connection.setClientID(activeMQConsumerName);
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    if (activeMQSourceType.equals("queue")) {
                        destination = session.createQueue(activeMQSourceName);
                    } else {
                        destination = session.createTopic(activeMQSourceName);
                    }
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to created a connection / channel", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }


                // define the queue
                MessageConsumer consumer;
                try {
                    if (activeMQCreateDurableConsumer && activeMQSourceType.equals("topic")) {
                        if (!"".equals(activeMQTopicFilterExpression)) {
                            consumer = session.createDurableSubscriber(
                                    (Topic) destination, // topic name
                                    activeMQConsumerName, // consumer name
                                    activeMQTopicFilterExpression, // filter expression
                                    true // ?? TODO - lookup java doc as soon as network connection is back.
                            );
                        } else {
                            consumer = session.createDurableSubscriber((Topic) destination, activeMQConsumerName);
                        }
                    } else {
//                        consumer = session.createConsumer(destination, activeMQConsumerName);
                        consumer = session.createConsumer(destination);
                    }

                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to create queue/topic [{}]", e, activeMQSourceName);
                    }
                    cleanup(0, "failed to create queue");
                    continue;
                }

                try {
                    connection.start();
                } catch (JMSException e) {
                    cleanup(5, "failed to start connection");
                }


                // now use the queue/topic to listen for messages
                while (true) {
                    if (closed) {
                        break;
                    }
                    Message message;
                    try {
                        message = consumer.receive();
                        logger.info("got a message [{}]", message);
                    } catch (Exception e) {
                        if (!closed) {
                            logger.error("failed to get next message, reconnecting...", e);
                        }
                        cleanup(0, "failed to get message");
                        break;
                    }
                    logger.info("check if message is of type textmessage");
                    if (message != null && message instanceof TextMessage) {
                        logger.info("it is of type textmessage");
                        final List<String> deliveryTags = Lists.newArrayList();

                        byte[] msgContent;
                        try {
                            TextMessage txtMessage = (TextMessage) message;

                            msgContent = txtMessage.getText().getBytes();
                            logger.info("message was [{}]", txtMessage.getText());

                        } catch (Exception e) {
//                                logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getJMSCorrelationID() );
//                                try {
//                                    channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
//                                } catch (IOException e1) {
//                                    logger.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
//                                }
                            continue;
                        }

                        logger.info("preparing bulk");
                        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                        logger.info("bulk prepared.. ");

                        try {
                            logger.info("adding message to bulkRequestBuilder");
                            bulkRequestBuilder.add(msgContent, 0, msgContent.length, false);
                            logger.info("added message to bulkRequestBuilder");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        logger.info("adding deliveryTags");
                        try {
                            deliveryTags.add(message.getJMSMessageID());
                        } catch (JMSException e) {
                            logger.warn("failed to get JMS Message ID", e);
                        }

                        logger.info("checking if numberOfActions [{}] < bulkSize [{}]", bulkRequestBuilder.numberOfActions(), bulkSize);
                        if (bulkRequestBuilder.numberOfActions() < bulkSize) {
                            logger.info("it is..");
                            // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                            try {
//                                    while ((message = consumer.receive(bulkTimeout.millis())) != null) {
                                logger.info("trying to get more messages, waiting 2000l ");
                                while ((message = consumer.receive(bulkTimeout.millis())) != null) {
                                    try {
                                        byte[] content = ((TextMessage) message).getText().getBytes();
                                        bulkRequestBuilder.add(content, 0, content.length, false);
                                        deliveryTags.add(message.getJMSMessageID());
                                    } catch (Exception e) {
                                        logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, message.getJMSMessageID());
//                                            try {
//                                                channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
//                                            } catch (Exception e1) {
//                                                logger.warn("failed to ack on failure [{}]", e1, task.getEnvelope().getDeliveryTag());
//                                            }
                                    }
                                    if (bulkRequestBuilder.numberOfActions() >= bulkSize) {
                                        break;
                                    }
                                }
//                                } catch (InterruptedException e) {
//                                    if (closed) {
//                                        break;
//                                    }
                            } catch (JMSException e) {
                                logger.info("catched an exception [{}]", e);
                                e.printStackTrace();
                            }
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                        }

                        logger.info("if is ordered... ");
                        if (ordered) {
                            logger.info("it is ordered.. ");
                            try {
                                BulkResponse response = bulkRequestBuilder.execute().actionGet();
                                if (response.hasFailures()) {
                                    // TODO write to exception queue?
                                    logger.warn("failed to execute" + response.buildFailureMessage());
                                }
                                for (String deliveryTag : deliveryTags) {
//                                        try {
//                                            channel.basicAck(deliveryTag, false);
//                                        } catch (Exception e1) {
//                                            logger.warn("failed to ack [{}]", e1, deliveryTag);
//                                        }
                                }
                            } catch (Exception e) {
                                logger.warn("failed to execute bulk", e);
                            }
                        } else {
                            bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                                @Override
                                public void onResponse(BulkResponse response) {
                                    if (response.hasFailures()) {
                                        // TODO write to exception queue?
                                        logger.warn("failed to execute" + response.buildFailureMessage());
                                    }
                                    for (String deliveryTag : deliveryTags) {
//                                            try {
//                                                channel.basicAck(deliveryTag, false);
//                                            } catch (Exception e1) {
//                                                logger.warn("failed to ack [{}]", e1, deliveryTag);
//                                            }
                                    }
                                }

                                @Override
                                public void onFailure(Throwable e) {
                                    logger.warn("failed to execute bulk for delivery tags [{}], not ack'ing", e, deliveryTags);
                                }
                            });
                        }
                    } else {
                        logger.warn("it is not ... :(");
                    }
                }
            }
            cleanup(0, "closing river");
        }

        private void cleanup(int code, String message) {
            try {
                session.close();
            } catch (Exception e) {
                logger.debug("failed to close session on [{}]", e, message);
            }

            try {
                connection.stop();
            } catch (JMSException e) {
                logger.debug("failed to stop connection on [{}]", e);
            }

            try {
                connection.close();
            } catch (Exception e) {
                logger.debug("failed to close connection on [{}]", e, message);
            }
        }
    }

}
