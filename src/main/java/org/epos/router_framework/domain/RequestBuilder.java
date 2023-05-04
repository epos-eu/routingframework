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

import static org.epos.router_framework.util.ExceptionUtils.requireConditionOrElseThrowIAE;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;

public class RequestBuilder {
	
	private final ServiceType serviceType;
	private final String operationType;
	private final String requestType;
	private Serializable payload;
	private PayloadType payloadType;
	private Map<String, Object> headers;
	private Optional<Duration> requestTTL = Optional.empty();
	
	private static final Duration DEFAULT_REQUEST_TTL = Duration.ofMillis(10_500);	// millisec
		
	public static RequestBuilder instance(ServiceType serviceType, String operationType, String requestType)
	{
		return new RequestBuilder(serviceType, operationType, requestType);
	}
	
	private RequestBuilder(ServiceType serviceType, String operationType, String requestType)
	{
		
		this.serviceType = requireConditionOrElseThrowIAE(serviceType, "ServiceType must be specified", Objects::nonNull);		
		this.operationType = requireConditionOrElseThrowIAE(operationType, "OperationType must be specified", StringUtils::isNotBlank);
		this.requestType = requireConditionOrElseThrowIAE(requestType, "Non-blank requestType must be specified", StringUtils::isNotBlank);
	}
	
	public RequestBuilder addHeaders(Map<String, Object> headers)
	{
		this.headers = (headers == null) ? new HashMap<>() : new HashMap<>(headers);
		return this;
	}
	
	public <T extends Map<String, String> & Serializable> RequestBuilder addPayloadPropertiesMap(T payload) {
		return addPayload(payload, PayloadType.PROPERTIES_MAP);
	}
	
	public RequestBuilder addPayloadPlainText(String payload) {
		return addPayload(payload, PayloadType.PLAIN_TEXT);
	}
	

	/**
	 * @param requestTTL in milliseconds
	 * @return this
	 * @throws IllegalArgumentException if passed in timeout value is not a positive number
	 */
	public RequestBuilder addRequestTTL(int requestTTL)
	{
		requireConditionOrElseThrowIAE(requestTTL, "Timeout must be a positive", n -> n > 0);
		this.requestTTL = Optional.of(Duration.ofMillis(requestTTL));
		return this;
	}
	
	private RequestBuilder addPayload(Serializable payload, PayloadType payloadType) 
	{
		if (this.payload != null) {
			throw new IllegalStateException("Attempt made to set payload more than once");
		}
		this.payload = requireConditionOrElseThrowIAE(payload, "Payload must be specified: cannot be null", Objects::nonNull);
		this.payloadType = payloadType;
		return this;
	}
	
	public Request build()
	{
		if (this.payload == null) {
			throw new IllegalStateException("Payload must be specified");
		}

		return new Request(serviceType, operationType, requestType, 
				payload, payloadType, headers, requestTTL.orElse(DEFAULT_REQUEST_TTL));				
	}

}
 
