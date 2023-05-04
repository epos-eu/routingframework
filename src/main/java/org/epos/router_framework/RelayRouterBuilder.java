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

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.handling.PlainTextRelayRouterHandlerFactory;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandlerFactory;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRouterBuilder {
	
	private static final Logger LOG = LoggerFactory.getLogger(RelayRouterBuilder.class);	
	private static final int DEFAULT_NUM_OF_PUBLISHERS = 1;
	
	private final Actor component;	
	private Optional<Integer> numOfPublishers = Optional.empty();
	private Optional<Integer> numOfConsumers = Optional.empty();
	private PropertiesMapRelayRouterHandlerFactory propertiesMapHandlerFactory;
	private PlainTextRelayRouterHandlerFactory plainTextHandlerFactory;
	
	private Map<ServiceType, Actor> supportedServices = new EnumMap<>(ServiceType.class);
	
	private RelayRouterBuilder(Actor component) 
	{
		this.component = component;
	}

	public static RelayRouterBuilder instance(Actor component) 
	{	
		return new RelayRouterBuilder(
				Objects.requireNonNull(component));
	}
	
	public RelayRouterBuilder setNumberOfPublishers(int numOfPublishers) 
	{
		this.numOfPublishers = Optional.of(numOfPublishers);
		return this;
	}
	
	public RelayRouterBuilder setNumberOfConsumers(int numOfConsumers) 
	{
		this.numOfConsumers = Optional.of(numOfConsumers);		
		return this;
	}
	
	public RelayRouterBuilder addServiceType(ServiceType serviceType, Actor nextComponent) 
	{		
		Objects.requireNonNull(serviceType);
		Objects.requireNonNull(nextComponent);
		
		supportedServices.put(serviceType, nextComponent);
		return this;
	}
	
	public RelayRouterBuilder addPropertiesMapPayloadTypeSupport(PropertiesMapRelayRouterHandlerFactory propertiesMapHandlerFactory) 
	{
		this.propertiesMapHandlerFactory = propertiesMapHandlerFactory;
		return this;
	}
	
	public RelayRouterBuilder addPlainTextPayloadTypeSupport(PlainTextRelayRouterHandlerFactory plainTextHandlerFactory) 
	{
		this.plainTextHandlerFactory = plainTextHandlerFactory;
		return this;
	}
	
	public Optional<RelayRouter> build() 
	{
		if (supportedServices.size() < 1) {
			if (LOG.isErrorEnabled()) {
				String errMsg = String.format("The DefaultRelayRouter must support at least 1 service but %d declared", supportedServices.size());
				LOG.error(errMsg);
			}
			return Optional.empty();
		}
		
		int calcNumOfPublishers = numOfPublishers.orElse(DEFAULT_NUM_OF_PUBLISHERS);
		int remainingProcessors = (Runtime.getRuntime().availableProcessors() * 2) - 1 - calcNumOfPublishers;
		int calcNumOfConsumers = numOfConsumers.orElse(remainingProcessors);
		
		DefaultRelayRouter router = new DefaultRelayRouter(component,
				calcNumOfPublishers, (calcNumOfConsumers > 0) ? calcNumOfConsumers : 1,
				supportedServices,
				propertiesMapHandlerFactory, plainTextHandlerFactory);
		
		return Optional.of(router);
	}
	
}
