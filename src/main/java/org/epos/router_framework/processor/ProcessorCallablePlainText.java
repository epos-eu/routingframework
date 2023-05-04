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

import org.epos.router_framework.handling.PlainTextRelayRouterHandler;
import org.epos.router_framework.types.ServiceType;

public class ProcessorCallablePlainText extends ProcessorCallable {
	
	private PlainTextRelayRouterHandler handler;

	public ProcessorCallablePlainText(ServiceType service, Map<String, Object> propertyMap, Object payload, PlainTextRelayRouterHandler handler) {
		super(service, propertyMap, payload);
		this.handler = handler;
	}

	@Override
	public ProcessorResult call() throws Exception {
		Serializable respPayload = handler.handle((String) payload, service, propertyMap);
		return createProcessorResult(respPayload, handler);
	}

}
