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
package org.epos.router_framework.domain;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;

public class Request {
	
	private final ServiceType serviceType;
	private final String operationType;
	private final String requestType;
	private Map<String, Object> headers;
	private Object payload;
	private PayloadType payloadType;
	private Duration requestTTL;
	
	/**
	 * @param requestTTL milliseconds
	 */
	Request(ServiceType serviceType, String operationType, String requestType, Object payload, PayloadType payloadType, 
			Map<String, Object> headers, Duration requestTTL)
	{
		this.serviceType = serviceType;
		this.operationType = operationType;
		this.requestType = requestType;
		this.payload = payload;
		this.payloadType = payloadType;
		this.headers = new HashMap<>(headers);
		this.requestTTL = requestTTL;
	}
	
	public Object getPayload() {
		return payload;
	}

	public PayloadType getPayloadType() {
		return payloadType;
	}

	public Map<String, Object> getHeaders() {
		return headers;
	}

	public ServiceType getServiceType() {
		return serviceType;
	}

	public String getOperationType() {
		return operationType;
	}

	public String getRequestType() {
		return requestType;
	}

	/**
	 * @return milliseconds
	 */
	public Duration getRequestTTL() {
		return requestTTL;
	}
	
}
