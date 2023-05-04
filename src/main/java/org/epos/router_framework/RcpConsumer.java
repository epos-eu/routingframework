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

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.domain.Response;
import org.epos.router_framework.types.ErrorCode;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.util.OneTimeKeyBlockingValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RcpConsumer extends DefaultConsumer {
	
	private static final Logger LOG = LoggerFactory.getLogger(RcpConsumer.class);
	
	private final Actor component;
	private final int offerToInMemQueueTimeOut;
	private final PayloadConverter payloadConverter = new PayloadConverter();
	private final OneTimeKeyBlockingValueMap<String, Response> consumedResponsesInMem;

	public RcpConsumer(Channel channel, Actor component, int offerToInMemQueueTimeOut, 
			OneTimeKeyBlockingValueMap<String, Response> consumedResponsesInMem) 
	{
		super(channel);
		this.component = component;
		this.offerToInMemQueueTimeOut = offerToInMemQueueTimeOut;
		this.consumedResponsesInMem = consumedResponsesInMem;
	}
	
	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) 
			throws IOException 
	{
		EposPropertiesAdaptor headers = EposPropertiesAdaptor.backedFrom(properties.getHeaders());
		headers.appendComponentToAudit(component);
		
		Response response = createResponse(headers, body);
		String corrId = properties.getCorrelationId();
		
		try {
			boolean responseQueued = consumedResponsesInMem.put(corrId, response, offerToInMemQueueTimeOut, TimeUnit.SECONDS);
			if (!responseQueued) {
				String warnMsg = String.format("RESPONSE TIMED OUT: "
						+ "Attempted to queue response onto internal blocking queue 'consumedResponsesInMem but failed%n'"
						+ "  [Correlation ID = '%s']", corrId);					
				LOG.warn(warnMsg);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			String errMsg = String.format("THREAD INTERRUPTED: "
					+ "Attempted to queue response onto internal blocking queue 'consumedResponsesInMem' but failed%n'"
					+ "  [Correlation ID = '%s']", corrId);
			LOG.error(errMsg);		// error as not expecting interrupts!
		}
		
		getChannel().basicAck(envelope.getDeliveryTag(), false);
	}

	
	private Response createResponse(EposPropertiesAdaptor headers, byte[] body) {
		
		String componentAudit = headers.getPrettyPrintComponentAudit();
		Optional<ErrorCode> errorCode = headers.getErrorCode();
		Optional<String> errorMessage = headers.getErrorMessage();
		
		try {
			PayloadType payloadType = headers.getPayloadType().orElse(null);
			Object payloadObj = payloadConverter.fromByteArray(payloadType, body);
			Optional<Object> payload = (body == null) ? Optional.empty() : Optional.ofNullable(payloadObj);
			
			if (errorCode.isEmpty()) {				
				return new Response(payload, payloadType, headers.asPropertyMap(), componentAudit);					
			} else {
				return new Response(errorCode.get(), errorMessage.orElse(""), componentAudit);	
			}				
		} catch (NoSuchElementException e) {
			return new Response(ErrorCode.INTERNAL_ERROR, e.getMessage(), componentAudit);
		}
	}

}
