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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.epos.router_framework.PayloadConverter;
import org.epos.router_framework.types.ErrorCode;
import org.epos.router_framework.types.PayloadType;

public class Response {
	
	private final Optional<Object> payload;
	private PayloadType payloadType;
	private String componentAudit;
	private final Map<String, Object> headers;
	private Optional<ErrorCode> errorCode;
	private Optional<String> errorMessage;
	
	protected final PayloadConverter payloadConverter = new PayloadConverter();
	
	public Response(ErrorCode errorCode, String errorMessage, String componentAudit) {	
		this(Optional.empty(), null, Collections.emptyMap(), Optional.ofNullable(errorCode), Optional.ofNullable(errorMessage), componentAudit);
	}

	public Response(Optional<Object> payload, PayloadType payloadType, Map<String, Object> headers, String componentAudit) {
		this(payload, payloadType, headers, Optional.empty(), Optional.empty(), componentAudit);
	}
	
	protected Response(Optional<Object> payload, PayloadType payloadType, Map<String, Object> headers, Optional<ErrorCode> errorCode, Optional<String> errorMessage, String componentAudit) 
	{	
		this.payload = payload;
		this.payloadType = payloadType;
		this.headers = headers;
		this.errorCode = errorCode;
		this.errorMessage = errorMessage;
		this.componentAudit = componentAudit;
	}

	public Optional<Map<String, String>> getPayloadAsPropertiesMap()
	{
		if (!PayloadType.PROPERTIES_MAP.equals(payloadType)) {
			throw new UnsupportedOperationException("Payload cannot be converted to a property map");
		}
		return payload.filter(Map.class::isInstance)
				      .map(Map.class::cast);
	}
	
	public Optional<String> getPayloadAsPlainText()
	{
		if (!PayloadType.PLAIN_TEXT.equals(payloadType)) {
			throw new UnsupportedOperationException("Payload cannot be converted to plain text");
		}
		return payload.filter(String.class::isInstance)
				      .map(String.class::cast);
	}
	
	public Optional<ErrorCode> getErrorCode() {
		return errorCode;
	}
	
	/* TODO propose that we avoid directly exposing the AMQP headers to the client code */
	public Object getHeaderValue(String paramName) {
		if (paramName == null) return null;
		return headers.get(paramName);
	}
	
	public boolean hasHeaderValue(String paramName) {
		if (paramName == null) return false;
		return headers.containsKey(paramName) && headers.get(paramName) != null;
	}

	public PayloadType getPayloadType() {
		return payloadType;
	}

	public Optional<String> getErrorMessage() {
		return errorMessage;
	}

	public String getComponentAudit() {
		return componentAudit;
	}

	@Override
	public String toString() {
		return "Response [payload=" + payload + ", payloadType=" + payloadType + ", componentAudit=" + componentAudit
				+ ", headers=" + headers + ", errorCode=" + errorCode + ", errorMessage=" + errorMessage
				+ ", payloadConverter=" + payloadConverter + "]";
	}

}
