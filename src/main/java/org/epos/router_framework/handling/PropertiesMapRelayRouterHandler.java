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
package org.epos.router_framework.handling;

import java.io.Serializable;
import java.util.Map;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.exception.RoutingMessageHandlingException;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;

public abstract class PropertiesMapRelayRouterHandler extends RelayRouterHandler {

	public PropertiesMapRelayRouterHandler(Actor defaultNextActor) {
		super(defaultNextActor, PayloadType.PROPERTIES_MAP);
	}

	public abstract Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException;

}
