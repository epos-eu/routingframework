/*******************************************************************************
 * Copyright 2021 valerio
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.epos.mq;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * MQManager is the connector among components and RabbitMQ.
 * It generates:
 *  1. Real connection (Connection)
 *  2. Virtual connections (Channels)
 *  3. Create listeners (Consumers) for each virtual connection (2)
 *  
 * The version 2.0 creates only the payload of the message, the system is decentralized and the management takes informations from RabbitMQ
 * directly
 * 
 * @author Valerio Vinciarelli INGV
 * @version 2.0
 * @deprecated use router-framework instead
 */
public class Manager 
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Manager.class);
	
	private Connection connectionToRabbitMQ = null;
	private ArrayList<Channel> channels;
	private String sendQueue, receiveQueue, rabbitName;
	private Handler handler;

	/**
	 * Constructor
	 * 
	 * @param sendQueue __
	 * @param receiveQueue __
	 * @param rabbitName __
	 * @param handler __
	 */
	public Manager(String sendQueue,String receiveQueue,String rabbitName, Handler handler)
	{
		this.sendQueue = sendQueue;
		this.receiveQueue = receiveQueue;
		this.rabbitName = System.getenv("BROKER_HOST");
		this.handler = handler;
	}

	/**
	 * 
	 * @throws IOException  __
	 * @throws TimeoutException  __
	 * 
	 */
	public void init() throws IOException, TimeoutException
	{
		createConnection();
		declareChannels();
		createConsumers();
	}

	/**
	 * Create connection to RabbitMQ host
	 * 
	 * @throws IOException __
	 * @throws TimeoutException __
	 */
	private void createConnection() throws IOException, TimeoutException
	{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(System.getenv("BROKER_HOST"));
		factory.setUsername(System.getenv("BROKER_USERNAME"));
		factory.setPassword(System.getenv("BROKER_PASSWORD"));
		factory.setVirtualHost(System.getenv("BROKER_VHOST"));
		connectionToRabbitMQ = factory.newConnection();
	}

	/**
	 * Declares 2 channels and a queue for each channel
	 * 
	 * @throws IOException __
	 * @throws TimeoutException 
	 */
	private void declareChannels() throws IOException, TimeoutException 
	{
		channels = new ArrayList<>();
		for(int i = 0; i<2; i++) {
			
			try (Channel channel = connectionToRabbitMQ.createChannel()) {
				channel.queueDeclare(receiveQueue, false, false, false, null);
				channel.basicQos(1);
				channels.add(channel);
			}
		}
	}

	/**
	 * Creates a consumer for each channel, a consumer is a listener/thread
	 * 
	 * @throws IOException __
	 * @throws TimeoutException __
	 */
	private synchronized void createConsumers() throws IOException, TimeoutException {
		for (final Channel c : channels) {
			Consumer consumer = new DefaultConsumer(c) {
				@Override
				public void handleDelivery(final String consumerTag, final Envelope envelope,
						final AMQP.BasicProperties properties, final byte[] body) throws IOException {
					Runnable thread = new Runnable() {
						public void run() {
							BasicProperties replyProps = new BasicProperties.Builder()
									.correlationId(properties.getCorrelationId()).replyTo(receiveQueue).build();
							LOGGER.debug(
									"[.] Message Received from MessageQueue \nID: " + properties.getCorrelationId());
							try {
								byte[] message = handler.handle(new String(body, "UTF-8")).getBytes("UTF-8");
								LOGGER.debug(handler.getSendQueue());
								c.basicPublish("", handler.getSendQueue(), replyProps, message);
								c.basicAck(envelope.getDeliveryTag(), false);
							} catch (UnsupportedEncodingException e) {
								LOGGER.error("Problem creating consumers", e);
							} catch (IOException e) {
								LOGGER.error("Problem creating consumers", e);
							}
						}
					};
					thread.run();
				}
			};

			c.basicConsume(receiveQueue, false, consumer);
		}
	}

	/**
	 * @return the sendQueue
	 */
	public String getSendQueue() {
		return sendQueue;
	}

	/**
	 * @param sendQueue the sendQueue to set
	 */
	public void setSendQueue(String sendQueue) {
		this.sendQueue = sendQueue;
	}

	/**
	 * @return the rabbitName
	 */
	public String getRabbitName() {
		return rabbitName;
	}

	/**
	 * @param rabbitName the rabbitName to set
	 */
	public void setRabbitName(String rabbitName) {
		this.rabbitName = rabbitName;
	}

}
