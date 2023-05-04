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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.epos.router_framework.RpcRouter;
import org.epos.router_framework.RpcRouterBuilder;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.domain.BuiltInActorType;
import org.epos.router_framework.domain.RequestBuilder;
import org.epos.router_framework.domain.Response;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LOAD_WITH_DELAY_GENERIC_RpcRouterSimulator {

	private static final Logger LOG = LoggerFactory.getLogger(LOAD_WITH_DELAY_GENERIC_RpcRouterSimulator.class);
	
	/**
	 * @param args <ul>
	 * <li>#0 - number of routing publisher threads</li>
	 * <li>#1 - number of routing consumer threads</li>
	 * <li>#2 - number of repetitions for given request</li>
	 * <li>#3 - Relay Router's processor thread TTL (milliseconds)</li>
	 * <li>#4 - expire time (milliseconds) for request</li>
	 * </ul>
	 * 
	 */
	public static void main(String[] args) {
		
		LOG.info("Starting RCPRouter instance simulation (simple requests with delays)");
		
		String host = System.getenv("BROKER_HOST");
		String vhost = System.getenv("BROKER_VHOST");
		String username = System.getenv("BROKER_USERNAME");
		String password = System.getenv("BROKER_PASSWORD");
		
		int numOfPublishers = Integer.parseInt(args[0]);
		int numOfConsumers = Integer.parseInt(args[1]);
		int requestRepetitions = Integer.parseInt(args[2]);
		int processTtlMilliSeconds = Integer.parseInt(args[3]);
		int requestExpiryTimeMilliSeconds = Integer.parseInt(args[4]);
		
		LOG.info("Number of request repititions = {}", requestRepetitions);
		LOG.info(String.format("Expiry time for requests = %d.%03d seconds", 
				requestExpiryTimeMilliSeconds / 1_000, 
				requestExpiryTimeMilliSeconds % 1_000));
		
		// initialise component-wide Router instance
		Optional<RpcRouter> router = RpcRouterBuilder.instance(Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get())
			.addServiceSupport(ServiceType.EXTERNAL, Actor.getInstance(BuiltInActorType.TCS_CONNECTOR.verbLabel()).get())
			.setNumberOfPublishers(numOfPublishers)
			.setNumberOfConsumers(numOfConsumers)
			.setTtlInProcessor(processTtlMilliSeconds)
			.setRoutingKeyPrefix("api")
			.build();
		
		RpcRouter _router = router.orElseThrow();
		
		try {
			_router.init(host, vhost, username, password);
		} catch (RoutingException e) {
			throw new RuntimeException(e);
		}
		
		// model REST service calls coming in on separate threads (users)
		ExecutorService executors = Executors.newFixedThreadPool(30, 
				th -> new Thread(th, "Initator " + UUID.randomUUID().toString()));
		
		String[] requestPayloads = { 
				"one","two","three","four","five","six","seven","eight","nine","ten",
				"eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen","twenty",
				"twenty-one","twenty-two","twenty-three","twenty-four","twenty-five","twenty-six","twenty-seven","twenty-eight","twenty-nine",
				"thirty","thirty-one","thirty-two","thirty-three","thirty-four","thirty-five","thirty-six","thirty-seven","thirty-eight","thirty-nine",
				"forty","forty-one","forty-two","forty-three","forty-four","forty-five","forty-six","forty-seven","forty-eight","forty-nine",
				"fifty","fifty-one","fifty-two","fifty-three","fifty-four","fifty-five","fifty-six","fifty-seven","fifty-eight","fifty-nine",
				"sixty","sixty-one","sixty-two","sixty-three","sixty-four","sixty-five","sixty-six","sixty-seven","sixty-eight","sixty-nine",
				"seventy","seventy-one","seventy-two","seventy-three","seventy-four","seventy-five","seventy-six","seventy-seven","seventy-eight",
				"seventy-nine","eighty","eighty-one","eighty-two","eighty-three","eighty-four","eighty-five","eighty-six","eighty-seven","eighty-eight","eighty-nine",
				"ninety","ninety-one","ninety-two","ninety-three","ninety-four","ninety-five","ninety-six","ninety-seven","ninety-eight","ninety-nine","one-hundred"				
			};
		final int numOfRequestRuns = requestPayloads.length * requestRepetitions;
		
		List<Future<Response>> futures = new ArrayList<>();
		AtomicInteger totalNumOfRequests = new AtomicInteger(0);
		 
		long startTime = System.nanoTime();
		
		IntStream.range(0, numOfRequestRuns).forEach(i -> {
			
			/*
			 * PAYLOAD TYPE = PropertiesMap;
			 * NEXT COMPONENT = service's default next component
			 */			
			futures.add(executors.submit(() -> {
				Map<String, String> requestPayload = Map.of("param1", requestPayloads[i % requestPayloads.length],
															"param2", "<another-value>",
															"param3", "<and-yet-another-value>");
				
				int requestFlavourIdx = totalNumOfRequests.get() % numOfRequestRuns;
				Map<String, Object> requestHeaders = Map.of("header1", String.format("LOAD_WITH_DELAY_GENERIC_RpcRouterSimulator: %d", requestFlavourIdx),
															"header2", "I expect to change!");
				
				return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
															.addPayloadPropertiesMap((Map<String, String> & Serializable) requestPayload)
															.addHeaders(requestHeaders)
															.addRequestTTL(requestExpiryTimeMilliSeconds)
															.build());
			}));
			totalNumOfRequests.getAndAdd(1);
			
			/*
			 * PAYLOAD TYPE = PropertiesMap;
			 * NEXT COMPONENT = explicitly declare next component
			 */			
			futures.add(executors.submit(() -> {
				Map<String, String> requestPayload = Map.of("param1", requestPayloads[i % requestPayloads.length],
															"param2", "<another-value>",
															"param3", "<and-yet-another-value>");
				
				int requestFlavourIdx = totalNumOfRequests.get() % numOfRequestRuns;
				Map<String, Object> requestHeaders = Map.of("header1", String.format("LOAD_WITH_DELAY_GENERIC_RpcRouterSimulator: %d", requestFlavourIdx),
															"header2", "I expect to change!");
				
				return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
															.addPayloadPropertiesMap((Map<String, String> & Serializable) requestPayload)
															.addHeaders(requestHeaders)
															.addRequestTTL(requestExpiryTimeMilliSeconds)
															.build(), Actor.getInstance(BuiltInActorType.TCS_CONNECTOR.verbLabel()).get());
			}));
			totalNumOfRequests.getAndAdd(1);
			
			/*
			 * PAYLOAD TYPE = PlainText;
			 * NEXT COMPONENT = service's default next component
			 */			
			futures.add(executors.submit(() -> {
				int requestFlavourIdx = totalNumOfRequests.get() % numOfRequestRuns;
				Map<String, Object> requestHeaders = Map.of("header1", String.format("LOAD_WITH_DELAY_GENERIC_RpcRouterSimulator: %d", requestFlavourIdx),
															"header2", "I expect to change!");
				
				return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
															.addPayloadPlainText(requestPayloads[i % requestPayloads.length])
															.addHeaders(requestHeaders)
															.addRequestTTL(requestExpiryTimeMilliSeconds)
															.build());
			}));
			totalNumOfRequests.getAndAdd(1);
			
			/*
			 * PAYLOAD TYPE = PlainText;
			 * NEXT COMPONENT = explicitly declare next component
			 */			
			futures.add(executors.submit(() -> {
				int requestFlavourIdx = totalNumOfRequests.get() % numOfRequestRuns;
				Map<String, Object> requestHeaders = Map.of("header1", String.format("LOAD_WITH_DELAY_GENERIC_RpcRouterSimulator: %d", requestFlavourIdx),
															"header2", "I expect to change!");
				
				return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
															.addPayloadPlainText(requestPayloads[i % requestPayloads.length])
															.addHeaders(requestHeaders)
															.addRequestTTL(requestExpiryTimeMilliSeconds)
															.build(), Actor.getInstance(BuiltInActorType.TCS_CONNECTOR.verbLabel()).get());
			}));
			totalNumOfRequests.getAndAdd(1);
			
		});
	
	
		// Wait for responses to come back
		int i = 0;
		int numWithErrorCodes = 0;
		
	    for(Future<Response> fut : futures) {
	        try {
	            //print the return value of Future, notice the output delay in console
	            // because Future.get() waits for task to get complete
	        	Response response = fut.get();
	        	
            	if (response.getErrorCode().isPresent()) {
    				numWithErrorCodes++;
    				
            		if (LOG.isDebugEnabled()) {
            				LOG.debug(response.getComponentAudit());
            				LOG.debug(String.format("[ERROR: %s] %s",
            						response.getErrorCode().get().name(),
            						response.getErrorMessage().isPresent() ? response.getErrorMessage().get() : "NONE"));
            		}
            	} else if (LOG.isDebugEnabled()) {
            		
    				switch (response.getPayloadType()) {
    				case PLAIN_TEXT : {
        				String responseReport = String.format(        						
        						"%n%s%n" +
        						"--- RETURN PLAIN TEXT PAYLOAD ---%n" +
        						"%s%n" +
        						"--- RETURN HEADER ---%n" +
        						"header1 = '%s'%n" +
        						"header2 = '%s'",
        						response.getComponentAudit(),
        						response.getPayloadAsPlainText().get(), 
        						response.getHeaderValue("header1"),
        						response.getHeaderValue("header2"));
        				LOG.debug(responseReport);
        				break;
    				}
					case PROPERTIES_MAP : {
						Map<String, String> resultMap = response.getPayloadAsPropertiesMap().get();
						String mapAsStr = resultMap.entrySet().stream()
							.map(e -> e.getKey()+"="+e.getValue())
							.collect(Collectors.joining(System.lineSeparator()));						
        				String responseReport = String.format(
        						"%n%s%n" +
        						"--- RETURN PROPERTIES-MAP PAYLOAD ---%n" +
        						"%s%n" +
        						"--- RETURN HEADER ---%n" +
        						"header1 = '%s'%n" +
        						"header2 = '%s'",
        						response.getComponentAudit(),
        						mapAsStr, 
        						response.getHeaderValue("header1"),
        						response.getHeaderValue("header2"));
						LOG.debug(responseReport);
						break;
					}
					default:
						break;	    
				}
            	}
            	i++;
            	
	        } catch (InterruptedException | ExecutionException e) {
	            e.printStackTrace();
	        }
	    }
	    
        long endTime = System.nanoTime();
        long executionTime = endTime - startTime;
        LOG.info(String.format("Execution time: %s", durationFormatUpToSeconds(Duration.ofNanos(executionTime))));
        LOG.info(String.format("Total requests: %d", totalNumOfRequests.get()));
        LOG.info(String.format("Total responses: %d", i));
        LOG.info(String.format("Total responses (Error Codes): %d", numWithErrorCodes));
        LOG.info(String.format("Average: %d req-resp/sec", i / (executionTime/1_000_000_000)));
	}
	
	private static String durationFormatUpToSeconds(Duration duration) 
	{
		long milliSec = duration.toMillis();
		return String.format("%d.%03d seconds", milliSec / 1_000, milliSec % 1_000);
	}
		    
}
