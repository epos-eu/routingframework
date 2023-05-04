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
package org.epos.router_framework.processor;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.exception.RoutingFrameworkUsageException;
import org.epos.router_framework.handling.RelayRouterHandler;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;

public abstract class ProcessorCallable implements Callable<ProcessorResult> {

	protected ServiceType service;
	protected Map<String, Object> propertyMap;
	protected Object payload;
	
	public ProcessorCallable(ServiceType service, Map<String, Object> propertyMap, Object payload) {
		this.service = service;
		this.propertyMap = propertyMap;
		this.payload = payload;
	}
	
	protected ProcessorResult createProcessorResult(Serializable respPayload, RelayRouterHandler handler) throws RoutingFrameworkUsageException
	{
		PayloadType respPayloadType = handler.getNextPayloadType();
		Actor overriddentNextActor = handler.getOverriddenNextActor()
				.orElse(handler.getDefaultNextActor());
		return new ProcessorResult(respPayload, respPayloadType, overriddentNextActor);
	}

}
