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
import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.epos.router_framework.RelayRouter;
import org.epos.router_framework.RelayRouterBuilder;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.exception.RoutingMessageHandlingException;
import org.epos.router_framework.handling.PlainTextRelayRouterHandler;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandler;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class RelayRouterSimulator_REQUEST_PAYLOAD_COMPARISON {
	
	private static final Logger LOG = LoggerFactory.getLogger(RelayRouterSimulator_REQUEST_PAYLOAD_COMPARISON.class);
	
	private static final Gson GSON = new Gson();
	
	public static void main(String[] args) 
	{
		LOG.info("Starting simulation" );
		
		// base router config on passed in cmd line args
		final Actor actor = Actor.getInstance(args[0]).get();
		int numOfPublishers = Integer.parseInt(args[1]);
		int numOfConsumers = Integer.parseInt(args[2]);
		
		ServiceType[] serviceTypesToSupport = IntStream.range(3, args.length)
				.filter(i -> (i % 2) == 1)
				.mapToObj(i -> args[i].trim())
				.map(arg -> ServiceType.getInstance(arg).get())
				.toArray(ServiceType[]::new);
		
		Actor[] supportedServicesNextActors = IntStream.range(3, args.length)
				.filter(i -> (i % 2) == 0)
				.mapToObj(i -> args[i].trim())
				.map(arg -> Actor.getInstance(arg).get())
				.toArray(Actor[]::new);
		
		// instantiate component-wide Router instance
		final RelayRouterBuilder routerBuilder = RelayRouterBuilder.instance(actor);

		IntStream.range(0, serviceTypesToSupport.length).forEach(i -> {
			
			routerBuilder.addServiceType(serviceTypesToSupport[i], supportedServicesNextActors[i]);

			switch (actor.name()) {
				case "QUERY_GENERATOR" : {
					
					routerBuilder.addPropertiesMapPayloadTypeSupport((nextActor) -> new PropertiesMapRelayRouterHandler(nextActor) {

						@Override
						public Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException
						{
							var copy = payload.keySet().stream()
									.collect(Collectors.toMap(
											Function.identity(), 
											propName -> payload.get(propName).toString()));
							
							// convert back to JSON
							return GSON.toJson(copy);
						}

						@Override
						public PayloadType getNextPayloadType() {
							return PayloadType.PLAIN_TEXT;
						}

					});
					
					routerBuilder.addPlainTextPayloadTypeSupport((nextActor) -> new PlainTextRelayRouterHandler(nextActor) {

						@Override
						public Serializable handle(String payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
						{
							// convert from JSON
							Type propMapType = new TypeToken<Map<String, String>>() {}.getType();
							Map<String, String> payloadAsMap = GSON.fromJson(payload, propMapType);

							Map<String, String> copy = payloadAsMap.keySet().stream()
								.collect(Collectors.toMap(
										Function.identity(), 
										propName -> payloadAsMap.get(propName).toString()));
							
							// convert back to JSON
							return GSON.toJson(copy);
						}
						
					});
					
					break;
				}
				case "TCS_CONNECTOR":
				case "WORKSPACE":
				case "DB_CONNECTOR":
				case "CONVERTER":
				case "INGESTOR":
				case "VALIDATOR":
				case "WEB_API":
				default: {
					break;
				}			
			}
		});
		
		RelayRouter router = routerBuilder
				.setNumberOfPublishers(numOfPublishers)
				.setNumberOfConsumers(numOfConsumers)
				.build().orElseThrow();
		
		// initialise component-wide Router instance
		initialiseRouter(router);
		
	}

	private static void initialiseRouter(RelayRouter router) 
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
	

}
