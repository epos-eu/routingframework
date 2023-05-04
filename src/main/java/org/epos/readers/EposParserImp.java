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
package org.epos.readers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class EposParserImp<T> implements EposParser {

	private static final Logger LOGGER = LoggerFactory.getLogger(EposParserImp.class);

	@Override
	public Object parse(byte[] input, Class<?> class1) {
		Gson gson = new Gson();

		try (ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(input))) {
			Object o = in.readObject();
			return gson.fromJson(o.toString(), class1);
		} catch (ClassNotFoundException | IOException e) {
			LOGGER.error("Problem parsing json", e);
		}
		return null;
	}

	@Override
	public byte[] convertToBytes(Object message) {
		Gson gson = new Gson();
		String convertedMessage = gson.toJson(message);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try (ObjectOutput out = new ObjectOutputStream(bos)) {

			out.writeObject(convertedMessage);
			out.flush();
			byte[] messageBytes = bos.toByteArray();
			return messageBytes;
		} catch (IOException e) {
			LOGGER.error("Problem converting json to bytes", e);
		}
		return null;
	}

}
