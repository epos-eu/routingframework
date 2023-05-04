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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Represents AMQP message for publishing 
 *
 */
public class AMQPMessage {
	
	private static final Logger LOG = LoggerFactory.getLogger(AMQPMessage.class);
	
	private String nextRoutingKey;
	private String exchange;
	private BasicProperties props;
	private byte[] body;
	
	public AMQPMessage(String nextRoutingKey, String exchange, BasicProperties props, byte[] body) 
	{
		this.nextRoutingKey = nextRoutingKey;
		this.exchange = exchange;
		this.props = props;
		this.body = body.clone();	// https://www.artima.com/intv/bloch.html#part13
	}

	public String getNextRoutingKey() {
		return nextRoutingKey;
	}

	public String getExchange() {
		return exchange;
	}

	public BasicProperties getProps() {
		return props;
	}

	public byte[] getBody() {
		return body;
	}

}
