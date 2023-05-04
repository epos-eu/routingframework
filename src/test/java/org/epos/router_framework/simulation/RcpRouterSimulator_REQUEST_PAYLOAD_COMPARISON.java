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
package org.epos.router_framework.simulation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.epos.router_framework.RpcRouter;
import org.epos.router_framework.RpcRouterBuilder;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.domain.RequestBuilder;
import org.epos.router_framework.domain.Response;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import lombok.Lombok;

public class RcpRouterSimulator_REQUEST_PAYLOAD_COMPARISON {
	
	private static final Logger LOG = LoggerFactory.getLogger(RcpRouterSimulator_REQUEST_PAYLOAD_COMPARISON.class);
	private final static Gson GSON = new Gson();
	private final static Map<String, Object> ADDITIONAL_HEADERS = Map.of("header-property", "some-additional-header-value");
	
	public static void main(String[] args) 
	{
		LOG.info("Starting simulation" );
		
		// base router config on passed in cmd line args
		final String routingKeyPrefix = args[0];
		final Actor owningActor = Actor.getInstance(args[1]).get();
		final int numOfPublishers = Integer.parseInt(args[2]);
		final int numOfConsumers = Integer.parseInt(args[3]);
		final ServiceType serviceTypeToSupport = ServiceType.getInstance(args[4]).get();
		final Actor servicesNextActor = Actor.getInstance(args[5]).get();
		final PayloadType payloadTypeUnderTest = PayloadType.getInstance(args[6]).get();
		
		// instantiate component-wide Router instance
		RpcRouter router = RpcRouterBuilder.instance(owningActor)
			.setNumberOfPublishers(numOfPublishers)
			.setNumberOfConsumers(numOfConsumers)
			.setRoutingKeyPrefix(routingKeyPrefix)
			.addServiceSupport(serviceTypeToSupport, servicesNextActor)
			.build().orElseThrow();
		
		// initialise component-wide Router instance
		initialiseRouter(router);
		
		doSimulation(router, payloadTypeUnderTest);
	}
	
	private static void initialiseRouter(RpcRouter router) 
	{
		try {
			router.init(System.getenv("BROKER_HOST"), 
	        		System.getenv("BROKER_VHOST"), 
	        		System.getenv("BROKER_USERNAME"), 
	        		System.getenv("BROKER_PASSWORD"));
		} catch (RoutingException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private static void doSimulation(RpcRouter router, PayloadType payloadTypeUnderTest) 
	{
		final var opType = "get";
		final var requestType = "execute";
		final var service = router.getSupportedServices().iterator().next();
		
		List<Map<String, String>> payloadsAsMaps = List.of(	

			// EXTERNAL service: execution
			Map.of("service", "WFS",
					"minlongitude", "-180",
					"typenames", "gsmlp:BoreholeView",
					"maxlatitude", "90",
					"version", "2.0.2",
					"outputFormat", "json",
					"request", "GetFeature",
					"minlatitude", "-90",
					"maxlongitude", "180")
		);
		
		 List<String> payloadsAsStrings = payloadsAsMaps.stream()
			.map(GSON::toJson)
			.collect(Collectors.toList());
		
		ExecutorService executors = Executors.newFixedThreadPool(10, 
				th -> new Thread(th, "Initator " + UUID.randomUUID().toString()));
		
		List<Future<Response>> futures = new ArrayList<>();
		int repititions = 5000;
		long startTime = System.nanoTime();

		IntStream.range(0, payloadsAsMaps.size() * repititions).forEach(i -> {
			
			futures.add(executors.submit(() -> {
				RequestBuilder builder = RequestBuilder.instance(service, opType, requestType)
					.addHeaders(ADDITIONAL_HEADERS);
				
				switch (payloadTypeUnderTest) {
				case PROPERTIES_MAP : 
					Map<String, String> payload = payloadsAsMaps.get(i % payloadsAsMaps.size());
					builder.addPayloadPropertiesMap((Map<String, String> & Serializable) payload);
					break;
				case PLAIN_TEXT :
					builder.addPayloadPlainText(payloadsAsStrings.get(i % payloadsAsStrings.size()));
					break;
				default :
					throw new Exception("unknown payload type to test!");
				}
				
				return router.makeRequest(builder.build());
			}));
			
		});
		
		List<Response> responses = futures.stream()
//			.map(ExceptionUtils.rethrowFunction(Future::get))
			.map(t -> {
				try {
					return t.get();
				} catch (InterruptedException | ExecutionException e) {
					Lombok.sneakyThrow(e);
				}
				return null;
			})
			.collect(Collectors.toList());
		
        long endTime = System.nanoTime();
        long executionTime = endTime - startTime;
		
		responses.forEach(response -> {
			if (response.getErrorCode().isPresent()) {
				logErrorResponse(response);
			} else {
				logNormalResponse(response);
			}
		});

        LOG.info(String.format("Execution time: %d sec", executionTime/1_000_000_000));
        LOG.info(String.format("Execution time: %d ms", executionTime/1_000));
        LOG.info(String.format("Total responses: %d", responses.size()));
        LOG.info(String.format("Average: %d req-resp/sec", responses.size() / (executionTime/1_000_000_000)));
	}

	private static void logNormalResponse(Response response) 
	{
		if (LOG.isDebugEnabled()) {
			
			switch (response.getPayloadType()) {
				case PLAIN_TEXT : {
					String responseReport = String.format(
							"%s%n" +
							"===============================%n" +
							"-- RETURN PLAIN TEXT PAYLOAD --%n" +
							"%s%n" +
							"-- RETURN HEADER --%n" +
							"header-property = '%s'%n" +
							"===============================%n",
							response.getComponentAudit(),
							response.getPayloadAsPlainText().get(), 
							response.getHeaderValue("header-property"));
					LOG.debug(responseReport);
					break;
				}
				case PROPERTIES_MAP : {
					Map<String, String> resultMap = response.getPayloadAsPropertiesMap().get();
					String mapAsStr = resultMap.entrySet().stream()
						.filter(s -> !"myParam".equals(s))
						.map(e -> e.getKey()+"="+e.getValue())
						.collect(Collectors.joining(System.lineSeparator()));
					
					String responseReport = String.format(
							"%s%n" +
							"===============================%n" +
							"-- RETURN PROPERTIES-MAP PAYLOAD --%n" +
							"%s%n" +
							"-- RETURN HEADER --%n" +
							"header-property = '%s'%n" +
							"===============================%n",
							response.getComponentAudit(),
							mapAsStr, 
							response.getHeaderValue("header-property"));
					LOG.debug(responseReport);
					break;
				}
				default: {
					break;	    
				}
			}			
		}
	}

	private static void logErrorResponse(Response response)
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug(response.getComponentAudit());
			String errStr = String.format("[ERROR: %s] %s",
					response.getErrorCode().get().name(),
					response.getErrorMessage().isPresent() ? response.getErrorMessage().get() : "NONE");
			LOG.debug(errStr);
		}
	}



}
