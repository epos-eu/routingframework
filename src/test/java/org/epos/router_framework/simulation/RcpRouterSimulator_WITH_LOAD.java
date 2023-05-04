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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.epos.router_framework.domain.BuiltInActorType;
import org.epos.router_framework.domain.RequestBuilder;
import org.epos.router_framework.domain.Response;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RcpRouterSimulator_WITH_LOAD {

	private static final Logger LOG = LoggerFactory.getLogger(RcpRouterSimulator_WITH_LOAD.class);

	public static void main(String[] args) {
		LOG.info("Starting simulation");

		// initialise component-wide Router instance
		Optional<RpcRouter> router = RpcRouterBuilder.instance(Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get())			
				.addServiceSupport(ServiceType.METADATA, Actor.getInstance(BuiltInActorType.QUERY_GENERATOR.verbLabel()).get())
				.addServiceSupport(ServiceType.WORKSPACE, Actor.getInstance(BuiltInActorType.WORKSPACE.verbLabel()).get())
				.addServiceSupport(ServiceType.EXTERNAL, Actor.getInstance(BuiltInActorType.QUERY_GENERATOR.verbLabel()).get())
				.addServiceSupport(ServiceType.INGESTOR, Actor.getInstance(BuiltInActorType.INGESTOR.verbLabel()).get())
				.setNumberOfPublishers(1)
				.setNumberOfConsumers(1)
				.setRoutingKeyPrefix("api")
				.build();

		RpcRouter _router = router.orElseThrow();

		String host = System.getenv("BROKER_HOST");
		String vhost = System.getenv("BROKER_VHOST");
		String username = System.getenv("BROKER_USERNAME");
		String password = System.getenv("BROKER_PASSWORD");

		try {
			_router.init(host, vhost, username, password);
		} catch (RoutingException e) {
			throw new RuntimeException(e);
		}

		// model REST service calls coming in on separate threads
		ExecutorService executors = Executors.newFixedThreadPool(10, 
				th -> new Thread(th, "Initator " + UUID.randomUUID().toString()));

		String[] messages = { 
				"one","two","three","four","five","six","seven","eight","nine","ten",
				"eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen","twenty",
				"twenty-one","twenty-two","twenty-three","twenty-four","twenty-five","twenty-six","twenty-seven","twenty-eight","twenty-nine",
				"thirty","thirty-one","thirty-two","thirty-three","thirty-four","thirty-five","thirty-six","thirty-seven","thirty-eight","thirty-nine",
				"forty","forty-one","forty-two","forty-three","forty-four","forty-five","forty-six","forty-seven","forty-eight","forty-nine",
				"fifty","fifty-one","fifty-two","fifty-three","fifty-four","fifty-five","fifty-six","fifty-seven","fifty-eight","fifty-nine"//,
				//			"sixty","sixty-one","sixty-two","sixty-three","sixty-four","sixty-five","sixty-six","sixty-seven","sixty-eight","sixty-nine",
				//			"seventy","seventy-one","seventy-two","seventy-three","seventy-four","seventy-five","seventy-six","seventy-seven","seventy-eight",
				//			"seventy-nine","eighty","eighty-one","eighty-two","eighty-three","eighty-four","eighty-five","eighty-six","eighty-seven","eighty-eight","eighty-nine",
				//			"ninety","ninety-one","ninety-two","ninety-three","ninety-four","ninety-five","ninety-six","ninety-seven","ninety-eight","ninety-nine","one-hundred"				
		};

		String[] ingestorService_LineOptions = { "single", "multiple" };

		String[] metadataService_keywordOptions = { "Y2F0YWxvZ3Vlcw%3D%3D", "c3R1ZGllcw%3D%3D", "bWV0YWRhdGE%3D", "YmlibGlvZ3JhcGh5%2CbWV0YWRhdGE%3D" };

		String[] metadataService_distributionIds = { 
				"https%3A%2F%2Fcatalog.terradue.com%2Fgep-epos%2FSatelliteObservations%2FDataset%2FWrapped_Interferogram%2FDistribution%2FWrap_Interf",
				"https%3A%2F%2Fcatalog.terradue.com%2Fgep-epos%2FSatelliteObservations%2FDataset%2FSpatial_Coherence%2FDistribution%2FSpac_Coh"
		};

		List<Future<Response>> futures = new ArrayList<>();

		long startTime = System.nanoTime();

		int REPS = 1;

		IntStream.range(0, messages.length * REPS).forEach(i -> {

			// Discovery - Search Operation
			futures.add(executors.submit(() -> {		
				Map<String, String> payload = Map.of("keywords", metadataService_keywordOptions[i % metadataService_keywordOptions.length],
						"q", messages[i % messages.length]);
				Map<String, Object> headers = Map.of("header-property", "search-operation");

				return _router.makeRequest(
						RequestBuilder.instance(ServiceType.METADATA, "get", "resources.search")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload)
						.addHeaders(headers)
						.addRequestTTL(2)
						.build());						
			}));

			// Discovery - Metadata Resources Details #1
			futures.add(executors.submit(() -> {				
				Map<String, String> payload = Map.of("id", metadataService_distributionIds[i % metadataService_distributionIds.length]);
				Map<String, Object> headers = Map.of("header-property", "metadata resources details");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.METADATA, "get", "resources.details")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build());
			}));

			// Ingestor
			futures.add(executors.submit(() -> {
				Map<String, String> payload = Map.of("multiline", messages[i % ingestorService_LineOptions.length],
						"url", "file://localhost/" + messages[i % messages.length]);
				Map<String, Object> headers = Map.of("header-property", "ingestor-from-web-api");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.INGESTOR, "post", "internal.ingestor")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build());
			}));

			// Workspace
			futures.add(executors.submit(() -> {
				Map<String, String> payload = Map.of("myParam", messages[i % messages.length],
						"myMsg", "[" + Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get().name() + "]");
				Map<String, Object> headers = Map.of("header-property", "header-prop#2_value");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.WORKSPACE, "get", "workspaces.workspacesconfig")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build());
			}));

			// Discovery - Metadata Resources Details #2
			futures.add(executors.submit(() -> {
				Map<String, String> payload = Map.of("myParam", messages[i % messages.length],
						"myMsg", "[" + Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get().name() + "]");
				Map<String, Object> headers = Map.of("header-property", "header-prop#3_value");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.METADATA, "get", "resources.details")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build());
			}));


			// Workspace
			futures.add(executors.submit(() -> {
				Map<String, String> payload = Map.of("myParam", messages[i % messages.length],
						"myMsg", "[" + Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get().name() + "]");
				Map<String, Object> headers = Map.of("header-property", "header-prop#4_value");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.WORKSPACE, "post", "workspaces.workspacesconfig")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build());
			}));

			// EXTERNAL: Default route / Payload response
			futures.add(executors.submit(() -> {
				Map<String, String> payload = Map.of("myParam", messages[i % messages.length],
						"myMsg", "[" + Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get().name() + "]",
						"my-epos-response", "payload",
						"my-epos-manufacture-crisis", "false");
				Map<String, Object> headers = Map.of("header-property", "header-prop#5_value");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build());
			}));

			// EXTERNAL: Default route / Redirection URL response
			futures.add(executors.submit(() -> {
				Map<String, String> payload = Map.of("myParam", messages[i % messages.length],
						"myMsg", "[" + Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get().name() + "]",
						"my-epos-response", "redirection");
				Map<String, Object> headers = Map.of("header-property", "header-prop#6_value");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build());
			}));

			// EXTERNAL: Overridden route / Redirection URL response
			futures.add(executors.submit(() -> {
				Map<String, String> payload = Map.of("myParam", messages[i % messages.length],
						"myMsg", "[" + Actor.getInstance(BuiltInActorType.WEB_API.verbLabel()).get().name() + "]",
						"my-epos-response", "redirection");
				Map<String, Object> headers = Map.of("header-property", "header-prop#5_value");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
						.addPayloadPropertiesMap((Map<String, String> & Serializable) payload).addHeaders(headers).build(), 
						Actor.getInstance(BuiltInActorType.WORKSPACE.verbLabel()).get());
			}));

			// INGESTOR
			futures.add(executors.submit(() -> {
				//				Map<String, String> payload = Map.of("multiline", messages[i % ingestorService_LineOptions.length],
				//													 "url", "file://localhost/" + messages[i % messages.length]);
				HashMap<String, String> payload = new HashMap<String, String>();
				payload.put("multiline", messages[i % ingestorService_LineOptions.length]);
				payload.put("url", "file://localhost/" + messages[i % messages.length]);

				Map<String, Object> headers = Map.of("header-property", "ingestor-from-web-api");

				return _router.makeRequest(RequestBuilder.instance(ServiceType.INGESTOR, "post", "internal.ingestor")
						.addPayloadPropertiesMap(payload).addHeaders(headers).build());
			}));

		});

		// print responses as they come back
		int i = 0;
		int numWithErrorCodes = 0;

		for(Future<Response> fut : futures){
			try {
				//print the return value of Future, notice the output delay in console
				// because Future.get() waits for task to get complete
				Response response = fut.get();

				if (response.getErrorCode().isPresent()) {
					numWithErrorCodes++;
				}

				if (LOG.isDebugEnabled()) {
					if (response.getErrorCode().isPresent()) {
						LOG.debug(response.getComponentAudit());
						String errStr = String.format("[ERROR: %s] %s",
								response.getErrorCode().get().name(),
								response.getErrorMessage().isPresent() ? response.getErrorMessage().get() : "NONE");
						LOG.debug(errStr);
					} else {
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
						default:
							break;	    
						}

					}
				}
				i++;
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		long endTime = System.nanoTime();
		long executionTime = endTime - startTime;
		LOG.info(String.format("Execution time: %d sec", executionTime/1_000_000_000));
		LOG.info(String.format("Execution time: %d ms", executionTime/1_000));
		LOG.info(String.format("Total responses: %d", i));
		LOG.info(String.format("Total responses (Error Codes): %d", numWithErrorCodes));
		LOG.info(String.format("Average: %d req-resp/sec", i / (executionTime/1_000_000_000)));
	}

}
