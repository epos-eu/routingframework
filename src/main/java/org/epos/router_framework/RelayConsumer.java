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
import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.epos.router_framework.domain.AMQPMessage;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.exception.RoutingFrameworkUsageException;
import org.epos.router_framework.exception.RoutingMessageHandlingException;
import org.epos.router_framework.exception.RoutingTimeoutException;
import org.epos.router_framework.handling.PlainTextRelayRouterHandler;
import org.epos.router_framework.handling.PlainTextRelayRouterHandlerFactory;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandler;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandlerFactory;
import org.epos.router_framework.handling.RelayRouterHandler;
import org.epos.router_framework.processor.ProcessorCallablePlainText;
import org.epos.router_framework.processor.ProcessorCallablePropertiesMap;
import org.epos.router_framework.processor.ProcessorResult;
import org.epos.router_framework.types.ErrorCode;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RelayConsumer extends DefaultConsumer {
	
	private static final Logger LOG = LoggerFactory.getLogger(RelayConsumer.class);
	private static final int IN_MEMORY_QUEUE_OFFER_RETRIES_NUM = 3;
	
	private final Actor component;
	private final int offerToInMemQueueTimeOut;
	private final Map<ServiceType, Actor> supportedServicesMap;
	private final PropertiesMapRelayRouterHandlerFactory handlerFactoryPropertiesMap;
	private final PlainTextRelayRouterHandlerFactory handlerFactoryPlainText;
	private final String routeKeyReplacerRegex;
	private final BlockingQueue<AMQPMessage> pendingPublishQueue;
	private final PayloadConverter payloadConverter = new PayloadConverter();
	
	private final ExecutorService processorExecutor;
	
	public RelayConsumer(Channel channel, Actor component, int offerToInMemQueueTimeOut, 
			BlockingQueue<AMQPMessage> pendingPublishQueue, Map<ServiceType, Actor> supportedServicesMap,
			PropertiesMapRelayRouterHandlerFactory handlerFactoryPropertiesMap,
			PlainTextRelayRouterHandlerFactory handlerFactoryPlainText) 
	{
		super(channel);
		this.component = component;
		this.routeKeyReplacerRegex = String.format(".%s$", component.verbLabel());
		this.offerToInMemQueueTimeOut = offerToInMemQueueTimeOut;
		this.pendingPublishQueue = pendingPublishQueue;
		this.supportedServicesMap = supportedServicesMap;
		this.handlerFactoryPropertiesMap = handlerFactoryPropertiesMap;
		this.handlerFactoryPlainText = handlerFactoryPlainText;
		this.processorExecutor = Executors.newSingleThreadExecutor(th -> new Thread(th, String.format("%s_sub", Thread.currentThread().getName())));
	}
	
	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException
	{		
		EposPropertiesAdaptor headers = EposPropertiesAdaptor.backedFrom(properties.getHeaders());	
		
		// EXTRACT FROM REQUEST MESSAGE
		String correlationId = properties.getCorrelationId();
		
		headers.appendComponentToAudit(component);
		LOG.debug("Consuming message: Correlation ID = '{}'; component audit = '{}'", correlationId, headers.getPrettyPrintComponentAudit());
		
		ServiceType service = getServiceType(envelope.getExchange());
		final Actor defaultNextActor = supportedServicesMap.get(service);
		
		try {
			PayloadType reqPayloadType = headers.getPayloadType()
					.orElseThrow(() -> new RoutingException("Request Payload Type was absent"));
			Object reqPayload = payloadConverter.fromByteArray(reqPayloadType, body);

			ProcessorResult processorResult = getProcessorResult(headers, service, defaultNextActor, reqPayloadType, reqPayload);
	
			String nextRoutingKey = calculateNextRoutingKey(envelope.getRoutingKey(), 
					properties.getReplyTo(), 
					processorResult.getNextActor());
			
			headers.setPayloadType(processorResult.getRespPayloadType());
			
			BasicProperties props = new BasicProperties.Builder()
					.correlationId(correlationId)
					.headers(headers.asPropertyMap())
					.replyTo(properties.getReplyTo())
					.build();
			
			byte[] byteArrResp = convertResponseToByteArray(
					processorResult.getRespPayloadType(), 
					processorResult.getRespPayload());					
			AMQPMessage amqpMsg = new AMQPMessage(nextRoutingKey, envelope.getExchange(), props, byteArrResp);

			// PUBLISH RESPONSE MESSAGE
			if (publishResponseMessage(amqpMsg)) {
				getChannel().basicAck(envelope.getDeliveryTag(), false);
			} else {
				getChannel().basicNack(envelope.getDeliveryTag(), false, false);
			}

		} catch (TimeoutException e) {
			LOG.warn("Request with Correlation ID, '{}', Timed Out within client handler code", correlationId);
			returnErrorToSender(ErrorCode.TIMED_OUT, e, envelope, properties);
		} catch (RoutingTimeoutException e) {
			LOG.warn("Request with Correlation ID, '{}', Timed Out within Relay Router's handler code", correlationId);
			returnErrorToSender(ErrorCode.TIMED_OUT, e, envelope, properties);
		} catch (RoutingFrameworkUsageException e) {
			returnErrorToSender(ErrorCode.USAGE_ERROR, e, envelope, properties);
		} catch (RoutingException e) {
			returnErrorToSender(ErrorCode.INTERNAL_ERROR, e, envelope, properties);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error(String.format("Failed to send message assocated with Correlation ID, '%s', from component '%s", correlationId, component), e);
		} catch (ExecutionException e) {
			launderThrowable(e, envelope, properties);
		}

	}

	private ProcessorResult getProcessorResult(EposPropertiesAdaptor headers, ServiceType service, Actor defaultNextActor, 
			PayloadType reqPayloadType, Object reqPayload) throws RoutingException, InterruptedException, ExecutionException, TimeoutException 
	{		
		Callable<ProcessorResult> processor = getProcessor(headers, service, defaultNextActor, reqPayloadType, reqPayload);
		
		Optional<Duration> ttlInProcessor = headers.getTtlInProcessor();
		if (ttlInProcessor.isPresent()) {
			long ttlInProcessorMillisec = ttlInProcessor.get().toMillis();
			return processorExecutor.submit(processor).get(ttlInProcessorMillisec, TimeUnit.MILLISECONDS); /*, processor TimeOut (ultimately from 'expire' property of the request*/
		} else {
			return processorExecutor.submit(processor).get();
		}
	}
	

	private Callable<ProcessorResult> getProcessor(EposPropertiesAdaptor headers, ServiceType service, Actor defaultNextActor, 
			PayloadType reqPayloadType, Object reqPayload) throws RoutingException 
	{
		switch(reqPayloadType) {
		case PROPERTIES_MAP : 
			if (handlerFactoryPropertiesMap == null) {
				throw new RoutingFrameworkUsageException(
						String.format("No hanlder support has been provided for the Payload Type %s", 
						reqPayloadType.getLabel()));
			}
			PropertiesMapRelayRouterHandler propMaphandler = handlerFactoryPropertiesMap.getRelayRouterHandler(defaultNextActor);
			return new ProcessorCallablePropertiesMap(service, headers.asPropertyMap(), reqPayload, propMaphandler);							
		case PLAIN_TEXT:
			if (handlerFactoryPlainText == null) {
				throw new RoutingFrameworkUsageException(
						String.format("No hanlder support has been provided for the Payload Type %s",
						reqPayloadType.getLabel()));
			}
			PlainTextRelayRouterHandler plainTextHandler = handlerFactoryPlainText.getRelayRouterHandler(defaultNextActor);
			return new ProcessorCallablePlainText(service, headers.asPropertyMap(), reqPayload, plainTextHandler);		
		default:					
			throw new RoutingException(
					String.format("Payload label %s is not recognised", reqPayloadType.getLabel()));
		}
	}

	private void launderThrowable(ExecutionException ex, Envelope envelope, BasicProperties properties) 
	{
		Throwable cause = ex.getCause( );
		
		if (cause instanceof RoutingMessageHandlingException) {
			returnErrorToSender(ErrorCode.HANDLER_ERROR, cause, envelope, properties);
		}
		if (cause instanceof RoutingFrameworkUsageException || cause instanceof RuntimeException) {
			returnErrorToSender(ErrorCode.USAGE_ERROR, cause, envelope, properties);
		}
	}

	private boolean publishResponseMessage(AMQPMessage amqpMsg) throws InterruptedException
	{
		String correlationId = amqpMsg.getProps().getCorrelationId();			
		boolean taken = false;
		
		try {
			for (int i=0; (i < IN_MEMORY_QUEUE_OFFER_RETRIES_NUM) && !taken; i++) {
				taken = pendingPublishQueue.offer(amqpMsg, offerToInMemQueueTimeOut, TimeUnit.SECONDS);
			}

			if (!taken && LOG.isErrorEnabled()) {
				LOG.error("Failed to successfully offer message to publishing queue [Correlation ID: {}]{}"
						+ "{} retries attempted{}"
						+ "Size of publishing queue: {}",
						correlationId, System.lineSeparator(),
						IN_MEMORY_QUEUE_OFFER_RETRIES_NUM, System.lineSeparator(),
						pendingPublishQueue.size());								
			}
			
			return taken;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new InterruptedException(String.format(
					"Attempted to queue response onto internal blocking queue 'toPublishQueue' but failed%n'"
					+ "  [Correlation ID = '%s']%n", 
					correlationId));
		}
	}

	private void returnErrorToSender(ErrorCode errorCode, Throwable e, Envelope msgEnvelope, BasicProperties msgProperties) 
	{
		String exMsg = e.getMessage();
		String errMsg = String.format("Creating Error Response: Service = %s; Component = %s%n"
				+ "  Correlation ID = '%s'%n  %s: %s",
				msgEnvelope.getExchange(),
				component.name(),
				msgProperties.getCorrelationId(),
				e.getClass().getName(),
				StringUtils.isBlank(exMsg) ? "<no further detail available>" : exMsg);
		
		LOG.warn(errMsg);
		
		String returnRoutingKey = calculateReturnRoutingKey(msgEnvelope.getRoutingKey(), msgProperties.getReplyTo());
		EposPropertiesAdaptor headers = EposPropertiesAdaptor.backedFrom(msgProperties.getHeaders());
		
		headers.setPayloadType(PayloadType.EMPTY);
		headers.setErrorCode(errorCode);
		headers.setErrorMessage(errMsg);
		
		AMQPMessage amqpMsg = new AMQPMessage(returnRoutingKey, msgEnvelope.getExchange(), msgProperties, new byte[0]);
		
		try {
			boolean taken = pendingPublishQueue.offer(amqpMsg, offerToInMemQueueTimeOut, TimeUnit.SECONDS);
			if (!taken) {
				throw new RoutingTimeoutException("Submission to publish queue timed out");
			}
			getChannel().basicAck(msgEnvelope.getDeliveryTag(), false);
		} catch (IOException | RoutingTimeoutException ex) {
			LOG.error("Unable to return '{}' to sender (Correlation ID = '{}'){}{}", 
					errorCode,
					msgProperties.getCorrelationId(),
					System.lineSeparator(),
					ex.getStackTrace());
		} catch (InterruptedException e1) {
			LOG.error("Thread interrupted whilst try to queue up an error message to request's initiating component.%n"
					+ "Unable to return '{}' to sender (Correlation ID = '{}')",
					errorCode, msgProperties.getCorrelationId());
			Thread.currentThread().interrupt();
		}

	}

	private byte[] convertResponseToByteArray(PayloadType respPayloadType, Object respPayload) throws RoutingFrameworkUsageException
	{
		if (respPayload == null) {
			throw new RoutingFrameworkUsageException(
					String.format("Payloads returned from '%s's cannot be null", 
							RelayRouterHandler.class.getSimpleName()));
		}
		try {
			return payloadConverter.toByteArray(respPayloadType, respPayload);					
		} catch (IllegalArgumentException e) {
			throw new RoutingFrameworkUsageException(e.getMessage());
		}
	}
	
	private ServiceType getServiceType(String exchange) 
	{
		return ServiceType.getInstance(exchange).orElseThrow(() -> 
			new NoSuchElementException(String.format(
					"Service type, %s, cannot be determined for request being handled", 
					exchange)));
	}
	
	protected String calculateReturnRoutingKey(String currentRoutingKey, String replyToTag)
	{
		String responseSuffix;
		
		Optional<Actor> replyToComponent = Actor.getInstance(replyToTag);
		if (replyToComponent.isPresent()) {
			responseSuffix = replyToComponent.get().verbLabelReturnFlavour();
		} else {
			String errMsg = String.format(
					"Could not determine a routing key for response message.%n "
					+ "  ReplyTo component corresponding to action, '%s', could not be determined%n",
					replyToTag);
			LOG.error(errMsg);
			// TODO Cannot determine original requesting component: Send to a dead-letter queue?
			responseSuffix = "";
		}
		
		return currentRoutingKey.replaceFirst(routeKeyReplacerRegex, "." + responseSuffix);
	}

	protected String calculateNextRoutingKey(String currentRoutingKey, String replyToTag, Actor nextActor)
	{	
		String nextActorTag = nextActor.verbLabel();	
			
		if (nextActorTag.equals(replyToTag)) {
			return calculateReturnRoutingKey(currentRoutingKey, replyToTag);
		}
		return currentRoutingKey.replaceFirst(routeKeyReplacerRegex, "." + nextActorTag);
	}
	
}
