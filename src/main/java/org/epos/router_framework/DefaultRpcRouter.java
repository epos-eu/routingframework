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

import static org.epos.router_framework.util.ExceptionUtils.requireConditionOrElseThrowIAE;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.epos.router_framework.domain.AMQPMessage;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.domain.Request;
import org.epos.router_framework.domain.Response;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.exception.RoutingTimeoutException;
import org.epos.router_framework.types.ErrorCode;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;
import org.epos.router_framework.util.OneTimeKeyBlockingValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;

/**
 * Instance per ServiceType
 */
class DefaultRpcRouter extends RouterBase implements RpcRouter {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultRpcRouter.class);

	private final OneTimeKeyBlockingValueMap<String, Response> consumedResponsesInMem = new OneTimeKeyBlockingValueMap<>();
	private final Optional<String> routingKeyPrefix;
	private final Duration ttlInProcessor;
	
	private Connection publisherConn;
	private Connection consumerConn;
	
	DefaultRpcRouter(Actor component, int numOfPublishers, int numOfConsumers, Map<ServiceType, Actor> supportedServices, 
			Optional<String> routingKeyPrefix, Duration ttlInProcessor)
	{
		this(component, numOfPublishers, numOfConsumers, supportedServices, routingKeyPrefix, ttlInProcessor, false);
	}
		
	DefaultRpcRouter(Actor component, int numOfPublishers, int numOfConsumers, Map<ServiceType, Actor> supportedServices, 
			Optional<String> routingKeyPrefix, Duration ttlInProcessor, boolean msgPersistencyRequired) 
	{
		super(component, numOfPublishers, numOfConsumers, component.verbLabelReturnFlavour(), supportedServices, msgPersistencyRequired);
		this.routingKeyPrefix = routingKeyPrefix;
		this.ttlInProcessor = ttlInProcessor;
	}
	
	@Override
	public synchronized void init(String host, String vHost, String userName, String password) throws RoutingException 
	{
		super.init(host, vHost, userName, password);
		
		if (LOG.isInfoEnabled()) {	
			String infoMsg = String.format("%s-specific properties for '%s' component on virtual host '%s' have been initialised to%n  "
					+ "Max TTL in RPC Router processing threads: %s",	
					this.getClass().getSimpleName(),
					component, vHost, durationFormatUpToSeconds(ttlInProcessor));
			LOG.info(infoMsg);
		}
	}

	@Override
	protected void doInit(ConnectionFactory connFactory) throws RoutingException 
	{
		publisherConn = getConnection("RPCRouter Publisher connection ", connFactory);
		consumerConn = getConsumerConnection("RPCRouter Consumer connection", numOfConsumers, connFactory);
		
		startConsumers(consumerConn);
		startPublishers(publisherConn);
	}
	
	@Override
	public Response makeRequest(Request request) {
		return this.makeRequest(request, null);
	}

	@Override
	public Response makeRequest(Request request, Actor nextComponentOverride)
	{
		requireConditionOrElseThrowIAE(request, "Request object cannot be null", Objects::nonNull);
		
		if (!initialised) {
			throw new IllegalStateException("The router has not been initialised, therefore no requests can be made");
		}

		final String corrId = generateUID();		
		final Instant requestStartTime = Instant.now();
		final Duration requestTTL = request.getRequestTTL();
		
		AMQPMessage amqpMessage = constructAmqpMessage(request, nextComponentOverride, corrId, 
														requestStartTime, ttlInProcessor);
		
		if (LOG.isWarnEnabled() && request.getRequestTTL().minus(ttlInProcessor).isNegative()) {			
			LOG.warn("[Correlation ID={}] request TTL '{}' is shorter than RPC Router's TTL-in-Processor setting '{}'", 
					amqpMessage.getProps().getCorrelationId(),
					durationFormatUpToSeconds(request.getRequestTTL()),
					durationFormatUpToSeconds(ttlInProcessor));
		}

		try {
			// publish Request
			boolean taken = toPublishQueue.offer(amqpMessage, 
					getRequestTimeoutRemaining(requestStartTime, requestTTL, amqpMessage), 
					TimeUnit.MILLISECONDS);
			
			if (!taken) {
				String exMsg = getPublishingThreadTimeoutMsg(requestTTL, amqpMessage);
				throw new RoutingTimeoutException(exMsg);
			}
			
			// consume Response (n.b. thread blocked here whilst waiting!)
			Response response = consumedResponsesInMem.get(corrId,
					getRequestTimeoutRemaining(requestStartTime, requestTTL, amqpMessage),
					TimeUnit.MILLISECONDS);
			
			if (response == null) {
				String exMsg = getConsumingThreadTimeoutMsg(requestTTL, amqpMessage);
				throw new RoutingTimeoutException(exMsg);
			}

			return response;
		} catch (RoutingTimeoutException e) {
			logRequestDurationExceededTTL(requestTTL, requestStartTime);
			EposPropertiesAdaptor headers = EposPropertiesAdaptor.backedFrom(amqpMessage.getProps().getHeaders());
			String componentAudit = headers.getPrettyPrintComponentAudit();
			return new Response(ErrorCode.TIMED_OUT, e.getMessage(), componentAudit);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			
			String errMsg = String.format("ALERT! - Unexpected behaviour: an %s has been caught.%n"
					+ "  This may indicate an in-memory consumming or publishing queue has accumulated a stale message%n"
					+ "  [correlation ID = '%s']%n"								// NOSONAR (java:S1192) - want this duplicated as part of a format string!
					+ "  Current size of internal publish-request map = %d%n"	// NOSONAR (java:S1192) - want this duplicated as part of a format string!
					+ "  Current size of internal consume-response map = %d%n",	// NOSONAR (java:S1192) - want this duplicated as part of a format string!
					e.getClass().getName(),
					amqpMessage.getProps().getCorrelationId(),
					toPublishQueue.size(), consumedResponsesInMem.size());
			LOG.error(errMsg);
			
			EposPropertiesAdaptor headers = EposPropertiesAdaptor.backedFrom(amqpMessage.getProps().getHeaders());
			return new Response(
					ErrorCode.INTERNAL_ERROR, 
					e.getMessage(),
					headers.getPrettyPrintComponentAudit());
		}
	
	}

	private String durationFormatUpToSeconds(Duration duration) 
	{
		long milliSec = duration.toMillis();
		return String.format("%d.%03d seconds", milliSec / 1_000, milliSec % 1_000);
	}

	private AMQPMessage constructAmqpMessage(Request request, Actor nextComponentOverride, final String corrId,
			final Instant requestStartTime, final Duration ttlInProcessor) 
	{
		final EposPropertiesAdaptor headers = EposPropertiesAdaptor.backedFrom(request.getHeaders());
		headers.setTtlInProcessor(ttlInProcessor);
		headers.appendComponentToAudit(component);
		
		final String operationType = request.getOperationType();
		headers.setOperationType(operationType);
		
		final String requestType = request.getRequestType();
		headers.setRequestType(requestType);
		
		final PayloadType payloadType = request.getPayloadType();
		headers.setPayloadType(payloadType);

		final AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
				.correlationId(corrId)
				.replyTo(component.verbLabel())
				.timestamp(Date.from(requestStartTime))
//				.expiration(Long.toString(requestTTL.toMillis()))
				.headers(headers.asPropertyMap())
				.build();
		
		final ServiceType serviceType = request.getServiceType();
		
		if (!supportedServicesMap.containsKey(serviceType)) {
			String warnMsg = String.format("The service, %s, is not supported by the %s's %s instance", 
					serviceType.getServiceLabel(), component.verbLabel(), DefaultRpcRouter.class.getName());
			LOG.warn(warnMsg);
			throw new UnsupportedOperationException(warnMsg);
		}
		
		Actor nextComponent = nextComponentOverride != null ? nextComponentOverride : supportedServicesMap.get(serviceType);
	
		String routingKey = (routingKeyPrefix.isPresent()) ?
				String.format("%s.%s.%s.%s", routingKeyPrefix.get(), operationType, requestType, nextComponent.verbLabel()) :
				String.format("%s.%s.%s", operationType, requestType, nextComponent.verbLabel());
		
		return new AMQPMessage(
				routingKey, 
				serviceType.getServiceLabel(), 
				props,
				payloadConverter.toByteArray(payloadType, request.getPayload()));
	}

	private void logRequestDurationExceededTTL(Duration requestTTL, Instant requestStartTime) 
	{
		if (LOG.isWarnEnabled()) {
			Duration timeElapsed = Duration.between(requestStartTime, Instant.now());
			String totalRequestTimeStr = String.format("Request duration (%d ms) exceeded TTL (%d ms)", 
					timeElapsed.toMillis(),
					requestTTL.toMillis());
			LOG.warn(totalRequestTimeStr);
		}
	}
	
	private long getRequestTimeoutRemaining(Instant requestStartTime, Duration requestTTL, AMQPMessage amqpMessage) throws RoutingTimeoutException
	{	
		Duration timeElapsed = Duration.between(requestStartTime, Instant.now());
		Duration remainingTTL = requestTTL.minus(timeElapsed);
		
		if (remainingTTL.isNegative()) {
			String exMsg = getStandardTimeoutMsg(requestTTL, amqpMessage);
			throw new RoutingTimeoutException(exMsg);
		}
		
		return remainingTTL.toMillis();
	}
	
	private String getStandardTimeoutMsg(Duration requestTimeout, AMQPMessage amqpMessage) 
	{
		return String.format("Request for message exceeded the specified request timeout of %d milliseconds.%n"
						+ "  Was to be Sent to Exchange '%s' with RoutingKey of '%s'%n"		// NOSONAR - want to keep format text as a single string for readability
						+ "  [correlation ID = '%s']%n",	// NOSONAR - want to keep format text as a single string for readability
				requestTimeout.toMillis(),
				amqpMessage.getExchange(), amqpMessage.getNextRoutingKey(), 
				amqpMessage.getProps().getCorrelationId());
	}

	private String getPublishingThreadTimeoutMsg(Duration requestTimeout, AMQPMessage amqpMessage) 
	{
		return String.format("Request for message was not taken by publishing thread: Exceeded specified request timeout of %d milliseconds.%n"
						+ "  To be Sent to Exchange '%s' with RoutingKey of '%s'%n"
						+ "  [correlation ID = '%s']%n" 
						+ "  Current size of internal publish-request map = %d%n"
						+ "  Current size of internal consume-response map = %d%n",
				requestTimeout.toMillis(),
				amqpMessage.getExchange(), amqpMessage.getNextRoutingKey(), 
				amqpMessage.getProps().getCorrelationId(),
				toPublishQueue.size(), 
				consumedResponsesInMem.size());
	}
	
	private String getConsumingThreadTimeoutMsg(Duration requestTimeout, AMQPMessage amqpMessage) 
	{
		return String.format("Response for message was not obtained by consuming thread: Exceeded specified request timeout of %d milliseconds.%n"
						+ "  To be Sent to Exchange '%s' with RoutingKey of '%s'%n"
						+ "  [correlation ID = '%s']%n" 
						+ "  Current size of internal publish-request map = %d%n"
						+ "  Current size of internal consume-response map = %d%n",
				requestTimeout.toMillis(),
				amqpMessage.getExchange(), amqpMessage.getNextRoutingKey(), 
				amqpMessage.getProps().getCorrelationId(),
				toPublishQueue.size(), 
				consumedResponsesInMem.size());
	}

    private String generateUID() {
    	return new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()).toString();
    }

	@Override
	protected DefaultConsumer newConsumerCallback(Channel channel) {
		return new RcpConsumer(channel, component, offerToInMemQueueTimeOut, consumedResponsesInMem);
	}
	
	@Override
	public Boolean doHealthCheck() throws RoutingException 
	{
		String debugMsg = String.format("Started HealthCheck on connections to RabbitMQ of %s instance", DefaultRpcRouter.class.getName());
		LOG.debug(debugMsg);
		
		return getStatusConnection(publisherConn) && getStatusConnection(consumerConn);
	}
	
}
