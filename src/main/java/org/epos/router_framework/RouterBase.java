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
package org.epos.router_framework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.epos.router_framework.domain.AMQPMessage;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;

import lombok.Lombok;

public abstract class RouterBase implements Router {
	
	private static final Logger LOG = LoggerFactory.getLogger(RouterBase.class);

	private static final int QUEUE_THROTTLING_THRESHOLD = 500;
	private static final int IN_MEM_QUEUE_OFFER_TIMEOUT_SEC = 5;

	protected final ArrayBlockingQueue<AMQPMessage> toPublishQueue = new ArrayBlockingQueue<>(QUEUE_THROTTLING_THRESHOLD, true);
	protected final EnumMap<ServiceType, Actor> supportedServicesMap;
	protected final PayloadConverter payloadConverter = new PayloadConverter();
	
	protected final String monitoredQueueName;
	protected final Actor component;
	protected final String bindingKey;
	protected final int numOfPublishers;
	protected final int numOfConsumers;
	protected final int offerToInMemQueueTimeOut;
	protected final boolean msgPersistencyRequired;		// N.B. currently not being used!
	
	protected boolean initialised;
	protected int pollQueueTimeOut;
	
	public RouterBase(Actor component, int numOfPublishers, int numOfConsumers, String monitoredQueueName, Map<ServiceType, Actor> supportedServices, boolean msgPersistencyRequired) 
	{
		this.component = component;
		this.monitoredQueueName = monitoredQueueName;
		this.numOfPublishers = numOfPublishers;
		this.numOfConsumers = numOfConsumers;
		this.bindingKey = "#." + monitoredQueueName;
		this.supportedServicesMap = supportedServices.isEmpty() ? new EnumMap<>(ServiceType.class) : new EnumMap<>(supportedServices);
		this.msgPersistencyRequired = msgPersistencyRequired;
		this.offerToInMemQueueTimeOut = IN_MEM_QUEUE_OFFER_TIMEOUT_SEC;
	}

	public synchronized void init(String host, String vHost, String userName, String password) throws RoutingException
	{	
		validateMessageBrokerParams(host, vHost);
		
		Objects.requireNonNull(vHost);
		Objects.requireNonNull(userName);
		Objects.requireNonNull(password);
		
		if (initialised) {
			if (LOG.isInfoEnabled()) {
				String infoMsg = String.format("%s for '%s' component has ALREADY been initialised",
						this.getClass().getSimpleName(), 
						component);
				LOG.info(infoMsg);
			}
			return;
		}

		ConnectionFactory connFactory = getNewConnectionFactory(host, vHost, userName, password);		
		declareAmqpEntities(connFactory);
		
		doInit(connFactory);
		
		initialised = true;
		if (LOG.isInfoEnabled()) {	
			String infoMsg = String.format("%s for '%s' component has been initialised on virtual host '%s'%n  "
					+ "services supported: %s%n  "
					+ "queue being monitored: '%s'%n  "
					+ "# of publishers = %d%n  "
					+ "# of consumers = %d",	
					this.getClass().getSimpleName(),
					component, vHost,
					ServiceType.toString(Optional.ofNullable(supportedServicesMap.keySet())),
					monitoredQueueName,					
					numOfPublishers, numOfConsumers);
			LOG.info(infoMsg);
		}
	}
	
	@Override
	public Set<ServiceType> getSupportedServices() {
		return Collections.unmodifiableSet(supportedServicesMap.keySet());
	}
	
	public boolean hasInitialised() {
		return initialised;
	}
	
	protected abstract void doInit(ConnectionFactory connFactory) throws RoutingException;
	
	protected void declareAmqpEntities(final ConnectionFactory connFactory) throws RoutingException
	{
		try (Connection conn = getConnection("AMQP Entity declarations", connFactory); Channel channel = conn.createChannel()) {			
			declareComponentsRequiredExchanges(channel);
			declareComponentsMonitoredQueue(channel);			
			declareComponentsRequiredQueueBindings(channel);			
		} catch (TimeoutException e) {
			String warnMsg = String.format(
					"Failed to close the channel used to declare the AMQP Entities for the %s component's Directed Router",
					component.name());
			throw RoutingException.amqpEntityConfigIssue(warnMsg, e);
		} catch (IOException e) {
			throw RoutingException.channelNotObtained(e);
		}
	}

	private void declareComponentsRequiredQueueBindings(Channel channel) throws RoutingException	// NOSONAR - thrown sneakily
	{
		// Declare consumer queue bindings
		supportedServicesMap.keySet().forEach(serviceType -> {
			try {
				channel.queueBind(monitoredQueueName, serviceType.getServiceLabel(), bindingKey);
			} catch (IOException e) {
				String errMsg = String.format("Failed to configure binding, '%s, for queue, %s, to the exchange %s", 
						bindingKey,
						monitoredQueueName,
						serviceType.getServiceLabel());
				throw Lombok.sneakyThrow(RoutingException.amqpEntityConfigIssue(errMsg, e));
			}
		});
	}

	private void declareComponentsRequiredExchanges(Channel channel) throws RoutingException	// NOSONAR - thrown sneakily
	{
		supportedServicesMap.keySet().forEach(serviceType -> {
			try {
				channel.exchangeDeclare(serviceType.getServiceLabel(), "topic", true);
			} catch (IOException e) {
				String errMsg = String.format("Failed to declare exchange, '%s, for the %s service", 
						serviceType.getServiceLabel(),
						serviceType.name());
				throw Lombok.sneakyThrow(RoutingException.amqpEntityConfigIssue(errMsg, e));
			}
		});
	}

	private void declareComponentsMonitoredQueue(Channel channel) throws RoutingException
	{
		try {
			channel.queueDeclare(monitoredQueueName, true, false, false, null);
		} catch (IOException e) {
			String errMsg = String.format("Failed to configure queue, '%s", 
					monitoredQueueName);
			throw RoutingException.amqpEntityConfigIssue(errMsg, e);
		}
	}
	
	protected void logCompletedPublishers(Collection<Future<String>> publisherFutures) 
	{
		if (LOG.isInfoEnabled()) {	
			publisherFutures.forEach(future -> {
					try {
						LOG.info(future.get());
					} catch (InterruptedException e) {
						LOG.error("Interrupted during interrugation of completed publisher threads.");
						Thread.currentThread().interrupt();
					} catch (ExecutionException e) {
						LOG.error("Failed to interrugate completed publisher threads.");
					}
				});
		}
	}
	
	protected abstract DefaultConsumer newConsumerCallback(Channel channel);

	protected void startConsumers(final Connection conn) throws RoutingException	// NOSONAR - thrown sneakily
	{
		IntStream.range(0, numOfConsumers).forEach(i -> {
			try {
				waitAndConsume(conn);
			} catch (RoutingException e) {
				throw Lombok.sneakyThrow(e);
			}
		});
	}

	protected Collection<Future<String>> startPublishers(final Connection conn) 
	{	
		ExecutorService executor = Executors.newFixedThreadPool(numOfPublishers,
				th -> new Thread(th, conn.getClientProvidedName() + "_" + UUID.randomUUID().toString()));
		
		Collection<Future<String>> publisherFutures = new ArrayList<>(numOfPublishers);
		
		IntStream.range(0, numOfPublishers).forEach(i -> 			
			publisherFutures.add(executor.submit(() -> {
				waitAndPublish(conn);
				return String.format("%s component's message bus publishing thread, '%s', has ended.", 
						component.toString(),
						Thread.currentThread().getName());
			})
		));
		
		return publisherFutures;
	}
	
	protected Channel getChannel(Connection conn) throws RoutingException
	{
    	try {
    		Channel channel = conn.createChannel();    		
    		if (channel == null) {  // NOSONAR (java:S2583) - conn.createChannel() can return null; do understand SonarQube's complaint
    			throw RoutingException.channelNotObtained("none available");
    		}
    		return channel;
    	} catch (IOException e) {
    		String errMsg = String.format("I/O problem encountered (%s)", e.getMessage());
    		throw RoutingException.channelNotObtained(errMsg);
    	}
	}
	
	protected ConnectionFactory getNewConnectionFactory(String host, String vHost, String userName, String password)
	{	
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setVirtualHost(vHost);
		factory.setUsername(userName);
		factory.setPassword(password);
		return factory;
	}
	
	protected Connection getConnection(String connectionName, ConnectionFactory connFactory) throws RoutingException 
	{
		try {
			return connFactory.newConnection(connectionName);
		} catch (IOException | TimeoutException e) {
			throw RoutingException.connectionNotObtained(e);
		}
	}
	
	protected Boolean getStatusConnection(Connection connection) throws RoutingException 
	{
		return connection.isOpen();
	}
	
	protected Connection getConsumerConnection(String connectionName, int numOfThreads, ConnectionFactory connFactory) throws RoutingException 
	{
		ExecutorService es = Executors.newFixedThreadPool(
				numOfThreads, 
				th -> new Thread(th, String.format("%s %s", connectionName, UUID.randomUUID().toString()))
			);
		try {
			return connFactory.newConnection(es, connectionName);
		} catch (IOException | TimeoutException e) {
			throw RoutingException.connectionNotObtained(e);
		}
	}

	private void validateMessageBrokerParams(String host, String vHost) 
	{
		List<String> missingParams = new ArrayList<>();
		
		// mandatory parameters
		if (StringUtils.isBlank(host)) {
			missingParams.add("Host");
		}
		if (StringUtils.isBlank(vHost)) {
			missingParams.add("vHost");
		}
		if (StringUtils.isBlank(host)) {
			missingParams.add("username");
		}
		if (StringUtils.isBlank(vHost)) {
			missingParams.add("password");
		}

		if (!missingParams.isEmpty()) {
			String errFmt = "Values for the following message broker parameters were not specified:%n%s";
			throw new IllegalArgumentException(String.format(errFmt, String.join("; ", missingParams)));
		}
	}
	
	private void waitAndConsume(Connection conn) throws RoutingException
	{
    	Channel channel = getChannel(conn);
    	try {
    		channel.basicQos(1);	
    	} catch (IOException e) {
    		String errMsg = String.format("Could not configure 'quality of service' settings for channel (#%d).", channel.getChannelNumber());
    		throw RoutingException.amqpEntityConfigIssue(errMsg, e);
    	}
		
		try {
			channel.basicConsume(monitoredQueueName, false, newConsumerCallback(channel));
		} catch (IOException e) {
			throw RoutingException.failedToConsume(e);
		}
			
	}
	
	private void waitAndPublish(Connection conn) throws RoutingException
	{
		Channel channel = getChannel(conn);
		
		try {
			do {
				AMQPMessage amqpRequest = toPublishQueue.take();					
				publishRequest(channel, amqpRequest);					
			} while(true);
		} catch (InterruptedException e) {
			String msg = String.format("This %s component message bus publishing thread has been interrupted. [%s]", 
					component.toString(),
					e.toString());
			LOG.info(msg);
			Thread.currentThread().interrupt();
		}
	}

	private void publishRequest(Channel channel, AMQPMessage amqpRequest) {
		try {
			channel.basicPublish(amqpRequest.getExchange(), 
					amqpRequest.getNextRoutingKey(),
					amqpRequest.getProps(),
					amqpRequest.getBody());					
		} catch (IOException e) {
			LOG.error(String.format("%s component failed to publish message (%s) with routing key, %s.", 
					component.toString(),
					amqpRequest.getProps().getCorrelationId(),
					amqpRequest.getNextRoutingKey()));
		}
	}

}
