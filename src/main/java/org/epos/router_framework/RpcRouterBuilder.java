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

import static org.epos.router_framework.util.ExceptionUtils.requireConditionOrElseThrowIAE;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcRouterBuilder {
	
	private static final Logger LOG = LoggerFactory.getLogger(RpcRouterBuilder.class);
	
	private static final int DEFAULT_NUM_OF_PUBLISHERS = 1;
	private static final Duration DEFAULT_TTL_IN_PROCESSOR = Duration.ofMillis(10_000);	// millisec
	
	private Actor component;
	private int numOfPublishers = DEFAULT_NUM_OF_PUBLISHERS;
	private Integer numOfConsumers;
	private Optional<Duration> ttlInProcessor = Optional.empty();
	private Map<ServiceType, Actor> supportedServices = new EnumMap<>(ServiceType.class);
	
	private Optional<String> routingKeyPrefix = Optional.empty();
		
	private RpcRouterBuilder(Actor component) {		
		this.component = component;
	}

	public static RpcRouterBuilder instance(Actor component) {
		return new RpcRouterBuilder(Objects.requireNonNull(component));
	}
	
	public RpcRouterBuilder addServiceSupport(ServiceType service, Actor nextComponent) 
	{				
		supportedServices.put(
				Objects.requireNonNull(service), 
				Objects.requireNonNull(nextComponent));
		return this;
	}
	
	public RpcRouterBuilder setNumberOfPublishers(int numOfPublishers) 
	{
		this.numOfPublishers = numOfPublishers;
		return this;
	}
	
	public RpcRouterBuilder setNumberOfConsumers(int numOfConsumers) 
	{
		this.numOfConsumers = numOfConsumers;
		return this;
	}
	
	public RpcRouterBuilder setRoutingKeyPrefix(String routingKeyPrefix) {
		this.routingKeyPrefix = Optional.ofNullable(routingKeyPrefix);
		return this;
	}
	
	public RpcRouterBuilder setTtlInProcessor(int ttlInProcessor) {
		requireConditionOrElseThrowIAE(ttlInProcessor, "Timeout must be a positive", n -> n > 0);
		this.ttlInProcessor = Optional.of(Duration.ofMillis(ttlInProcessor));
		return this;
	}
	
	public Optional<RpcRouter> build()
	{		
		if (supportedServices.size() < 1) {
			if (LOG.isErrorEnabled()) {
				String errMsg = String.format("The RpcRouter must support at least 1 service but %d declared", supportedServices.size());
				LOG.error(errMsg);
			}		
			return Optional.empty();
		}
		
		RpcRouter router = new DefaultRpcRouter(component, numOfPublishers, 
				numOfConsumers != null ? numOfConsumers : getDefaultNumberOfConsumers(), 
				supportedServices, routingKeyPrefix,
				ttlInProcessor.orElse(DEFAULT_TTL_IN_PROCESSOR));
		
		return Optional.of(router);
	}

	private Integer getDefaultNumberOfConsumers() {
		return (Runtime.getRuntime().availableProcessors()*2)-1-numOfPublishers;
	}

}
