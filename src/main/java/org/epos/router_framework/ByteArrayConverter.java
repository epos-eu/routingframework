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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ByteArrayConverter {
	
	private static final Logger LOG = LoggerFactory.getLogger(ByteArrayConverter.class);
	
	public static final ByteArrayConverterForString STRING = new ByteArrayConverterForString();
	public static final ByteArrayConverterForMap MAP = new ByteArrayConverterForMap();
	
	private ByteArrayConverter() {
		throw new IllegalStateException("Utility class");
	}
	
	static class ByteArrayConverterForString {
		
		public String fromByteArray(byte[] body) {	
			return new String(body, StandardCharsets.UTF_8);
		}

		public byte[] toByteArray(String payload) {
			return payload.getBytes(StandardCharsets.UTF_8);
		}
	}
	
	static class ByteArrayConverterForMap  {
		
		@SuppressWarnings("unchecked")
		protected <T extends Map<? extends Serializable, ? extends Serializable> & Serializable> T fromByteArray(byte[] body) 
		{
			try {
				ObjectInputStream inBytes = new ObjectInputStream(new ByteArrayInputStream(body));
				return (T) inBytes.readObject();
			} catch (IOException | ClassNotFoundException e) {
				String errMsg = String.format("Cannot serialise payload: '%s'", e.getMessage());
				LOG.error(errMsg);
				throw new UnsupportedOperationException(errMsg, e);
			}
		}

		protected <T extends Map<? extends Serializable, ? extends Serializable> & Serializable> byte[] toByteArray(T payload) 
		{
		    try {
		    	ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
		    	ObjectOutputStream out = new ObjectOutputStream(byteArrayOS);
				out.writeObject(payload);
				return byteArrayOS.toByteArray();
			} catch (IOException e) {
				String errMsg = String.format("Cannot deserialise payload: '%s'", e.getMessage());
				LOG.error(errMsg);
				throw new UnsupportedOperationException(errMsg, e);
			}
		}
		
	}

}

