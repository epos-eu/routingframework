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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.epos.router_framework.RelayRouter;
import org.epos.router_framework.RelayRouterBuilder;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.domain.BuiltInActorType;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.exception.RoutingMessageHandlingException;
import org.epos.router_framework.handling.PlainTextRelayRouterHandler;
import org.epos.router_framework.handling.PlainTextRelayRouterHandlerFactory;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandler;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandlerFactory;
import org.epos.router_framework.types.PayloadType;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Considers RelayRouters for all components
 *
 */
public class RelayRouterSimulator_WITH_LOAD {
	
	private static final Logger LOG = LoggerFactory.getLogger(RelayRouterSimulator_WITH_LOAD.class);
	
	public static void main(String[] args) {
		LOG.info("Starting simulation" );
		
		final Actor actor = Actor.getInstance(args[0]).get();
		int numOfPublishers = Integer.parseInt(args[1]);
		int numOfConsumers = Integer.parseInt(args[2]);
		int additionalDelayMilliSeconds = Integer.parseInt(args[3]);

		ServiceType[] serviceTypesToSupport = IntStream.range(4, args.length)
				.filter(i -> (i % 2) == 0)
				.mapToObj(i -> args[i].trim())
				.map(arg -> ServiceType.getInstance(arg).get())
				.toArray(ServiceType[]::new);
		
		Actor[] supportedServicesNextActors = IntStream.range(4, args.length)
				.filter(i -> (i % 2) == 1)
				.mapToObj(i -> args[i].trim())
				.map(arg -> Actor.getInstance(arg).get())
				.toArray(Actor[]::new);
		
		final RelayRouterBuilder routerBuilder = RelayRouterBuilder.instance(actor);
		
		IntStream.range(0, serviceTypesToSupport.length).forEach(i -> {

			routerBuilder.addServiceType(serviceTypesToSupport[i], supportedServicesNextActors[i]);

			switch (actor.name()) {
			
				case "TCS_CONNECTOR" : {								
					PropertiesMapRelayRouterHandlerFactory propertiesMapRelayRouterHandlerFactory = new PropertiesMapRelayRouterHandlerFactory() {

						@Override
						public PropertiesMapRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
						{
							return new PropertiesMapRelayRouterHandler(defaultNextActor) {

								@Override
								public Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
								{
									HashMap<String, String> ret = new HashMap<>(payload);					
									String prevMsg = ret.get("myMsg");
									String newMsg = String.format("%s > [%s]", prevMsg, actor.name());
									ret.put("myMsg", newMsg);
									
									if (Boolean.parseBoolean(ret.get("my-epos-manufacture-crisis"))) {
										throw new RoutingMessageHandlingException("Cannot access TCS web service. And, yes, I did try!");
									}
									
									try {
										Thread.sleep(additionalDelayMilliSeconds);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}

									if (ret.get("my-epos-response").equals("payload")) {
										setNextPayloadType(PayloadType.PLAIN_TEXT);
										setOverriddenNextActor(Actor.getInstance(BuiltInActorType.CONVERTER.verbLabel()).get());							
										return newMsg + " {payload}";
									} else if (ret.get("my-epos-response").equals("redirection")) {
										ret.put("content-type", "text/html");	
										ret.put("httpStatusCode", "200");
										ret.put("redirect-url", "https://www.bbc.co.uk/news");
										return ret;
									}
									return "";
								}
								
							};
						}
						
					};					
					routerBuilder.addPropertiesMapPayloadTypeSupport(propertiesMapRelayRouterHandlerFactory);
					break;
				}
			
				case "WORKSPACE" : {
					PropertiesMapRelayRouterHandlerFactory propertiesMapRelayRouterHandlerFactory = new PropertiesMapRelayRouterHandlerFactory() {

						@Override
						public PropertiesMapRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
						{
							return new PropertiesMapRelayRouterHandler(defaultNextActor) {

								@Override
								public Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
								{
									HashMap<String, String> ret = new HashMap<>(payload);							
									String prevMsg = ret.get("myMsg");
									String newMsg = String.format("%s > [%s]", prevMsg, actor.name());
									ret.put("myMsg", newMsg);
									return ret;
								}
								
							};
						}
						
					};
					routerBuilder.addPropertiesMapPayloadTypeSupport(propertiesMapRelayRouterHandlerFactory);
					break;
				}
				
				case "QUERY_GENERATOR" : {
					PropertiesMapRelayRouterHandlerFactory propertiesMapRelayRouterHandlerFactory = new PropertiesMapRelayRouterHandlerFactory() {

						@Override
						public PropertiesMapRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
						{
							return new PropertiesMapRelayRouterHandler(defaultNextActor) {

								@Override
								public Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
								{
									if (ServiceType.EXTERNAL.equals(service)) {
										setOverriddenNextActor(Actor.getInstance(BuiltInActorType.DB_CONNECTOR.verbLabel()).get());
									}
									//	try {
									//		Thread.sleep(4_000);
									//	} catch (InterruptedException e) {
									//		// TODO Auto-generated catch block
									//		e.printStackTrace();
									//	}
									return (Serializable) Collections.unmodifiableMap(new HashMap<>(payload));
								}
								
							};
						}
						
					};
					routerBuilder.addPropertiesMapPayloadTypeSupport(propertiesMapRelayRouterHandlerFactory);
					break;
				}
				
				case "DB_CONNECTOR" : {
					PropertiesMapRelayRouterHandlerFactory propertiesMapRelayRouterHandlerFactory = new PropertiesMapRelayRouterHandlerFactory() {

						@Override
						public PropertiesMapRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
						{
							return new PropertiesMapRelayRouterHandler(defaultNextActor) {

								@Override
								public Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
								{
									if (ServiceType.METADATA.equals(service)) {
										return (Serializable) Collections.unmodifiableMap(new HashMap<>(payload));
									}
									
									Map<String, String> ret = new HashMap<>(payload);							
									String prevMsg = ret.get("myMsg");
									String newMsg = String.format("%s > [%s]", prevMsg, actor.name());
									ret.put("myMsg", newMsg);
									return (Serializable) ret;
								}

							};
						}						
					};
					routerBuilder.addPropertiesMapPayloadTypeSupport(propertiesMapRelayRouterHandlerFactory);
					break;
				}
				
				case "CONVERTER": {
					PlainTextRelayRouterHandlerFactory plainTextRelayRouterHandlerFactory = new PlainTextRelayRouterHandlerFactory() {

						@Override
						public PlainTextRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
						{
							return new PlainTextRelayRouterHandler(defaultNextActor) {

								@Override
								public Serializable handle(String payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
								{
									return String.format("%s > [%s]", payload, actor.name());
								}								
							};
						}
						
					};
					routerBuilder.addPlainTextPayloadTypeSupport(plainTextRelayRouterHandlerFactory);
					break;
				}
				
				case "INGESTOR": {
					PropertiesMapRelayRouterHandlerFactory propertiesMapRelayRouterHandlerFactory = new PropertiesMapRelayRouterHandlerFactory() {

						@Override
						public PropertiesMapRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
						{
							return new PropertiesMapRelayRouterHandler(defaultNextActor) {							
								@Override
								public Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
								{
									if (ServiceType.INGESTOR.equals(service)) {
										String multilineStatus = payload.get("multiline");
										String url = payload.get("url");
										return String.format("Multiline = '%s'%nUrl = '%s'", 
												multilineStatus, url);
									}
									return null;
								}
								
								@Override
								public PayloadType getNextPayloadType() {
									return PayloadType.PLAIN_TEXT;
								}								
							};
						}
						
					};
					routerBuilder.addPropertiesMapPayloadTypeSupport(propertiesMapRelayRouterHandlerFactory);
					break;
				}
				case "VALIDATOR":
				case "WEB_API":
				default: {
					break;
				}
			}

		});
		
			
		Optional<RelayRouter> router = routerBuilder
			.setNumberOfPublishers(numOfPublishers)
//			.setNumberOfConsumers(numOfConsumers)
			.build();
		
		RelayRouter _router = router.orElseThrow();
		
		try {
	        _router.init(System.getenv("BROKER_HOST"), 
	        		System.getenv("BROKER_VHOST"), 
	        		System.getenv("BROKER_USERNAME"), 
	        		System.getenv("BROKER_PASSWORD"));
			
		} catch (RoutingException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}
