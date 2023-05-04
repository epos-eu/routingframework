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

import java.io.Serializable;
import java.util.Map;

import org.epos.router_framework.types.PayloadType;

public class PayloadConverter {
	
	public byte[] toByteArray(PayloadType payloadType, Object payload) 
	{
		switch (payloadType) {
			case PROPERTIES_MAP :			
				return convertPayloadMapToByteArray(payload);
			case PLAIN_TEXT : 
				return convertPayloadTextToByteArray(payload);
			default : 
				throw new UnsupportedOperationException(
						String.format("Payload type (%s) is not supported", 
						payloadType.name()));
		}
	}
	
	public Object fromByteArray(PayloadType payloadType, byte[] payload)
	{		
		if (payloadType == null) {
			throw new UnsupportedOperationException("Payload type is missing");
		}
		
		switch (payloadType) {
		case PROPERTIES_MAP : 
			return convertPayloadMapFromByteArray(payload);
		case PLAIN_TEXT :
			return convertPayloadTextFromByteArray(payload);
		case EMPTY :
			return new byte[0];
		default :
			throw new UnsupportedOperationException(
					String.format("Payload type (%s) is not supported", 
					payloadType.name()));
		}
	}

	private byte[] convertPayloadTextToByteArray(Object payloadObj) 
	{
		if (payloadObj instanceof String) {
			return ByteArrayConverter.STRING.toByteArray((String) payloadObj);				
		}
		throw new IllegalArgumentException(
				String.format("Unexpected payload type of '%s'. Expecting an instance of '%s'", 
								payloadObj.getClass().getName(),
								String.class.getName()));
	}

	@SuppressWarnings("unchecked")
	private byte[] convertPayloadMapToByteArray(Object payloadObj) 
	{
		if (payloadObj instanceof Map && payloadObj instanceof Serializable) {
			requiresMapKeysValuesAsStrings((Map<?, ?> & Serializable) payloadObj);
			return ByteArrayConverter.MAP.toByteArray((Map<String, String> & Serializable) payloadObj);				
		}
		throw new IllegalArgumentException(
				String.format("Unexpected payload type of '%s'. Expecting an instance of '%s' '%s'", 
								payloadObj.getClass().getName(),
								Serializable.class.getName(),
								Map.class.getName()));
	}

	private String convertPayloadTextFromByteArray(byte[] payloadByteArr) {
		return ByteArrayConverter.STRING.fromByteArray(payloadByteArr);
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> convertPayloadMapFromByteArray(byte[] payloadByteArr) 
	{
		Map<?, ?> payload = ByteArrayConverter.MAP.fromByteArray(payloadByteArr);
		requiresMapKeysValuesAsStrings(payload);
		return (Map<String, String> & Serializable) payload;
	}
	
	/**
	 * @param payloadAsMap
	 * @return
	 * @throws IllegalArgumentException 
	 */
	private void requiresMapKeysValuesAsStrings(Map<?, ?> payloadAsMap) 
	{		
		boolean hasOnlyStringKeyValuePairs = payloadAsMap.entrySet().stream()
			.allMatch(e -> (e.getKey() instanceof String) && (e.getValue() instanceof String));
		
		if (!hasOnlyStringKeyValuePairs) {
			String errStr = String.format("Payloads of the type %s require keys and values of type %s", 
					Map.class.getSimpleName(), String.class.getSimpleName());
			throw new IllegalArgumentException(errStr);
		}
	}
	
}
