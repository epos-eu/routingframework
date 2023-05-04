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

import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import org.epos.router_framework.RelayRouterBuilder;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.exception.RoutingMessageHandlingException;
import org.epos.router_framework.handling.PlainTextRelayRouterHandler;
import org.epos.router_framework.handling.PlainTextRelayRouterHandlerFactory;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandler;
import org.epos.router_framework.handling.PropertiesMapRelayRouterHandlerFactory;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LOAD_WITH_DELAY_GENERIC_RelayRouterSimulator {
	
	private static final Logger LOG = LoggerFactory.getLogger(LOAD_WITH_DELAY_GENERIC_RelayRouterSimulator.class);
	
	/**
	 * @param args <ul>
	 * <li>#0 - number of routing publisher threads</li>
	 * <li>#1 - number of routing consumer threads</li>
	 * <li>#2 - name of component (must correspond to an Actor instance)</li>
	 * <li>#3 - additional delay time within handlers (milliseconds)</li>
	 * <li>#4 - supported service (paired with next Actor below)</li>
	 * <li>#5 - name of next component (pair with service above)</li>
	 * <li>#6 - ...</li>
	 * <li>#7 - ...</li>
	 * <li>#n-1 - supported service (paired with next Actor below)</li>
	 * <li>#n - name of next component (pair with service above)</li>
	 * </ul>
	 */
	public static void main(String[] args) {
		
		LOG.info("Starting DefaultRelayRouter instance simulation (simple requests with delays)");
		
		final int numOfPublishers = Integer.parseInt(args[0]);
		final int numOfConsumers = Integer.parseInt(args[1]);
		final Actor thisComponent = Actor.getInstance(args[2]).get();
		
		final int additionalDelayMilliSeconds = Integer.parseInt(args[3]);

		LOG.info(String.format("Additional artifical delay = %d.%03d seconds", 
				additionalDelayMilliSeconds / 1_000, 
				additionalDelayMilliSeconds % 1_000));
		
		final ServiceType[] serviceTypesToSupport = IntStream.range(4, args.length)
				.filter(i -> (i % 2) == 0)
				.mapToObj(i -> args[i].trim())
				.map(arg -> ServiceType.getInstance(arg).get())
				.toArray(ServiceType[]::new);
		
		final Actor[] supportedServicesNextActors = IntStream.range(4, args.length)
				.filter(i -> (i % 2) == 1)
				.mapToObj(i -> args[i].trim())
				.map(arg -> Actor.getInstance(arg).get())
				.toArray(Actor[]::new);
		
		final RelayRouterBuilder routerBuilder = RelayRouterBuilder.instance(thisComponent)
				.setNumberOfPublishers(numOfPublishers)
				.setNumberOfConsumers(numOfConsumers);
		
		IntStream.range(0, serviceTypesToSupport.length).forEach(i -> 
			routerBuilder.addServiceType(serviceTypesToSupport[i], supportedServicesNextActors[i]));
		
		// Handler for PropertiesMap - also returns payload as a PropertiesMap
		PropertiesMapRelayRouterHandlerFactory propertiesMapRelayRouterHandlerFactory = new PropertiesMapRelayRouterHandlerFactory() {

			@Override
			public PropertiesMapRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
			{
				return new PropertiesMapRelayRouterHandler(defaultNextActor) {

					@Override
					public Serializable handle(Map<String, String> payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
					{
						doArtificalDelay(additionalDelayMilliSeconds);

						// update header value
						if (headers.get("header2") != null) {
							String newVal = headers.get("header2") + " YOU'VE BEEN CHANGED!";
							headers.put("header2", newVal);
						}
						
						// create new payload
						return payload.entrySet().stream()
							.collect(toMap(Entry::getKey, e -> e.getValue() + " TOUCHED", (v1, v2) -> v2, HashMap::new));
					}
					
				};
			}
		};
		routerBuilder.addPropertiesMapPayloadTypeSupport(propertiesMapRelayRouterHandlerFactory);

		// Handler for PlainText - also returns payload as a PlainText
		PlainTextRelayRouterHandlerFactory plainTextRelayRouterHandlerFactory = new PlainTextRelayRouterHandlerFactory() {

			@Override
			public PlainTextRelayRouterHandler getRelayRouterHandler(Actor defaultNextActor) 
			{
				return new PlainTextRelayRouterHandler(defaultNextActor) {

					@Override
					public Serializable handle(String payload, ServiceType service, Map<String, Object> headers) throws RoutingMessageHandlingException 
					{
						doArtificalDelay(additionalDelayMilliSeconds);
						
						// update header value
						if (headers.get("header2") != null) {
							String newVal = headers.get("header2") + " YOU'VE BEEN CHANGED!";
							headers.put("header2", newVal);
						}
						
						// create new payload
						return payload + " TOUCHED";
					}
					
				};
			}
			
		};
		routerBuilder.addPlainTextPayloadTypeSupport(plainTextRelayRouterHandlerFactory);

		// Initialise the RelayRouter instance ready for use.
		try {
			routerBuilder.build().orElseThrow().init(System.getenv("BROKER_HOST"), 
	        		System.getenv("BROKER_VHOST"), 
	        		System.getenv("BROKER_USERNAME"), 
	        		System.getenv("BROKER_PASSWORD"));			
		} catch (RoutingException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	static void doArtificalDelay(int additionalDelayMilliSeconds) 
	{
		try {
			Thread.sleep(additionalDelayMilliSeconds);	// NOSONAR (java:S2925) - not considered relevant: this is a simulation code not a test with assertions
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
