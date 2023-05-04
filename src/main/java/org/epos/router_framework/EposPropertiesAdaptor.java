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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.types.ErrorCode;
import org.epos.router_framework.types.PayloadType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a properties map to serve as an adaptor for easy mutation/accessing of properties considered EPOS-specific
 * 
 * Also can be used to deal with type adaptation required to deal with limited types supported by AMQP
 */
public class EposPropertiesAdaptor {
	
	private static final Logger LOG = LoggerFactory.getLogger(EposPropertiesAdaptor.class);	
	private static final String EPOS_HEADER_KEY_PREFIX = "epos_";
	
	private enum EposPropertyKey {
		ERROR_MSG_KEY(EPOS_HEADER_KEY_PREFIX + "error-message"),
		OPERATION_TYPE_KEY(EPOS_HEADER_KEY_PREFIX + "operation-type"),
		REQUEST_TYPE_KEY(EPOS_HEADER_KEY_PREFIX + "request-type"),
		PAYLOAD_TYPE_KEY(EPOS_HEADER_KEY_PREFIX + "type-of-current-payload"),
		COMPONENT_AUDIT_TYPE_KEY(EPOS_HEADER_KEY_PREFIX + "component-audit"),
		ERROR_CODE_KEY(EPOS_HEADER_KEY_PREFIX + "error-code"),
		TTL_IN_PROCESSOR_KEY(EPOS_HEADER_KEY_PREFIX + "ttl-in-processor");;
		
		private final String key;
		
		private EposPropertyKey(String key) {
			this.key = key;
		}
	}
	
	private Map<String, Object> propsMap;
	
	protected static EposPropertiesAdaptor backedFrom(Map<String, Object> propertiesMap) {
		return new EposPropertiesAdaptor(propertiesMap);
	}

	private EposPropertiesAdaptor(Map<String, Object> propsMap) {
		this.propsMap = propsMap;
	}	
	
	protected Map<String, Object> asPropertyMap() {
		// n.b. already should be an unmodifiable view of the Property Map
		return Collections.unmodifiableMap(propsMap);
	}
	
	protected Map<String, Object> copyOfEposPropertySubset() {
		EnumSet<EposPropertyKey> eposKeys = EnumSet.allOf(EposPropertyKey.class);
		return eposKeys.stream()
				.map(e -> e.key)
				.filter(e -> propsMap.get(e) != null)				
				.collect(Collectors.toMap(Function.identity(), e -> propsMap.get(e)));
	}
	
	protected void setTtlInProcessor(Duration ttlMillis) {
		propsMap.put(EposPropertyKey.TTL_IN_PROCESSOR_KEY.key, ttlMillis.toMillis());
	}
	
	protected Optional<Duration> getTtlInProcessor()
	{
		Object ttlInProcessorLong = propsMap.get(EposPropertyKey.TTL_IN_PROCESSOR_KEY.key);
		return ttlInProcessorLong == null ? Optional.empty() : Optional.of(Duration.ofMillis((Long)ttlInProcessorLong));
	}

	protected void setErrorMessage(String errMsg) {
		propsMap.put(EposPropertyKey.ERROR_MSG_KEY.key, errMsg);
	}
	
	protected Optional<String> getErrorMessage()
	{
		if (propsMap.get(EposPropertyKey.ERROR_MSG_KEY.key) == null) return Optional.empty();
		return Optional.ofNullable(propsMap.get(EposPropertyKey.ERROR_MSG_KEY.key).toString());
	}
	
	protected void setOperationType(String operationType) {
		propsMap.put(EposPropertyKey.OPERATION_TYPE_KEY.key, operationType);
	}
	
	protected Optional<String> getOperationType()
	{
		Object val = propsMap.get(EposPropertyKey.OPERATION_TYPE_KEY.key);
		if (val instanceof String) {
			return Optional.of((String) val);
		}
		return Optional.empty();
	}

	protected void setPayloadType(PayloadType payloadType) {
		propsMap.put(EposPropertyKey.PAYLOAD_TYPE_KEY.key, payloadType.getLabel());
	}
	
	protected Optional<PayloadType> getPayloadType()
	{
		if (propsMap.get(EposPropertyKey.PAYLOAD_TYPE_KEY.key) == null) return Optional.empty();
				
		String reqPayloadLabel = propsMap.get(EposPropertyKey.PAYLOAD_TYPE_KEY.key).toString();
		Optional<PayloadType> reqPayloadType = PayloadType.getInstance(reqPayloadLabel);
		if (reqPayloadType.isEmpty()) {
			throw new NoSuchElementException(
					String.format("Payload label '%s' is not recognised", reqPayloadLabel));
		}
		return reqPayloadType;
	}
	
	protected void setErrorCode(ErrorCode code) {
		propsMap.put(EposPropertyKey.ERROR_CODE_KEY.key, code.getLabel());
	}
	
	protected Optional<ErrorCode> getErrorCode()
	{
		if (propsMap.get(EposPropertyKey.ERROR_CODE_KEY.key) == null) return Optional.empty();
		
		String errCodeLabel = propsMap.get(EposPropertyKey.ERROR_CODE_KEY.key).toString();
		Optional<ErrorCode> errorCode = ErrorCode.getInstance(errCodeLabel);
		if (errorCode.isEmpty()) {
			throw new NoSuchElementException(
					String.format("Error Code label '%s' is not recognised", errCodeLabel));
		}
		return errorCode;
	}
	
	protected void setRequestType(String requestType) {
		propsMap.put(EposPropertyKey.REQUEST_TYPE_KEY.key, requestType);
	}
	
	protected Optional<String> getRequestType()
	{
		Object val = propsMap.get(EposPropertyKey.REQUEST_TYPE_KEY.key);
		if (val instanceof String) {
			return Optional.of((String) val);
		}
		return Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	protected boolean appendComponentToAudit(Actor component)
	{
		 Object componentAudit = propsMap.computeIfAbsent(EposPropertyKey.COMPONENT_AUDIT_TYPE_KEY.key, k -> new ArrayList<String>());
		 
		 if (componentAudit instanceof List) {
			 return ((List<? super String>) componentAudit).add(component.name());
		 }
		 
		 LOG.error("[SYSTEM LOGIC ERROR] Could not append to '{}' property of properties map. Expected type of property key to be {} but was {}.",
				 EposPropertyKey.COMPONENT_AUDIT_TYPE_KEY.key, List.class.getName(), componentAudit.getClass().getName());
		 return false;	 
	}

	@SuppressWarnings("unchecked")
	protected String getPrettyPrintComponentAudit()
	{
		Object componentAudit = propsMap.getOrDefault(EposPropertyKey.COMPONENT_AUDIT_TYPE_KEY.key, new ArrayList<>());
		
		if (componentAudit instanceof List) {
			return ((List<? super String>) componentAudit).stream()
				.map(Object::toString)
				.collect(Collectors.joining(" > ", "[", "]"));
		}
		
		LOG.error("[SYSTEM LOGIC ERROR] Could not pretty print '{}' property of properties map. Expected type of property key to be {} but was {}.",
				EposPropertyKey.COMPONENT_AUDIT_TYPE_KEY.key, List.class.getName(), componentAudit.getClass().getName());
		return null;
	}

}
