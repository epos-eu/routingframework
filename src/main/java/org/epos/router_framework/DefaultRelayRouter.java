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

import java.util.Map;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.handling.PlainTextRelayRouterHandlerFactory;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandlerFactory;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;

/**
 * Instance per component ({@link Actor})
 *
 */
class DefaultRelayRouter extends RouterBase implements RelayRouter {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultRelayRouter.class);

	private final PropertiesMapRelayRouterHandlerFactory handlerFactoryPropertiesMap;
	private final PlainTextRelayRouterHandlerFactory handlerFactoryPlainText;
	
	private Connection publisherConn;
	private Connection consumerConn;
	 
	DefaultRelayRouter(Actor component, int numOfPublishers, int numOfConsumers, 
			Map<ServiceType, Actor> supportedServices,
			PropertiesMapRelayRouterHandlerFactory handlerFactoryPropertiesMap,
			PlainTextRelayRouterHandlerFactory handlerFactoryPlainText) 
	{
		this(component, numOfPublishers, numOfConsumers, supportedServices, handlerFactoryPropertiesMap, 
				handlerFactoryPlainText, false);
	}
	
	DefaultRelayRouter(Actor component, int numOfPublishers, int numOfConsumers, 
			Map<ServiceType, Actor> supportedServicesMap,
			PropertiesMapRelayRouterHandlerFactory handlerFactoryPropertiesMap,
			PlainTextRelayRouterHandlerFactory handlerFactoryPlainText,
			boolean msgPersistencyRequired)
	{		
		super(component, numOfPublishers, numOfConsumers, component.verbLabel(), 
				supportedServicesMap, msgPersistencyRequired);
		this.handlerFactoryPropertiesMap = handlerFactoryPropertiesMap;
		this.handlerFactoryPlainText = handlerFactoryPlainText;
	}
	
	@Override
	public synchronized void init(String host, String vHost, String userName, String password) throws RoutingException 
	{
		super.init(host, vHost, userName, password);
		
		if (LOG.isInfoEnabled()) {
			String infoMsg = String.format(
					"%n  Handler support for '%s' requests: %b%n"
					+ "  Handler support for '%s' requests: %b",
					PayloadType.PROPERTIES_MAP.getLabel(), handlerFactoryPropertiesMap != null,
					PayloadType.PLAIN_TEXT.getLabel(), handlerFactoryPlainText != null);
			LOG.info(infoMsg);
		}
	}

	
	@Override
	protected void doInit(ConnectionFactory connFactory) throws RoutingException 
	{
		Connection publisherConn = getConnection("ReplayRouter Publisher connection", connFactory);
		Connection consumerConn = getConsumerConnection("DefaultRelayRouter Consumer connection", numOfConsumers, connFactory);
		
		startPublishers(publisherConn);
		startConsumers(consumerConn);
	}

	@Override
	protected DefaultConsumer newConsumerCallback(Channel channel) {
		return new RelayConsumer(channel, component, 
				offerToInMemQueueTimeOut, toPublishQueue, supportedServicesMap, 
				handlerFactoryPropertiesMap, handlerFactoryPlainText);
	}
	
	@Override
	public Boolean doHealthCheck() throws RoutingException 
	{

		String debugMsg = String.format("Started HealthCheck on connections to RabbitMQ of %s instance", DefaultRpcRouter.class.getName());
		LOG.debug(debugMsg);
		
		return getStatusConnection(publisherConn) && getStatusConnection(consumerConn);
	}
	
}