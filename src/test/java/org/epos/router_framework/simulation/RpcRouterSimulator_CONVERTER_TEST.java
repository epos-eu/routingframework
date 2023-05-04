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

import static org.epos.router_framework.domain.BuiltInActorType.CONVERTER;
import static org.epos.router_framework.domain.BuiltInActorType.WEB_API;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.epos.router_framework.RpcRouter;
import org.epos.router_framework.RpcRouterBuilder;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.domain.RequestBuilder;
import org.epos.router_framework.domain.Response;
import org.epos.router_framework.exception.RoutingException;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RpcRouterSimulator_CONVERTER_TEST {
	
	private static final Logger LOG = LoggerFactory.getLogger(RpcRouterSimulator_CONVERTER_TEST.class);
	
	public static void main(String[] args) {
		
		// initialise component-wide Router instance
		Optional<RpcRouter> router = RpcRouterBuilder.instance(Actor.getInstance(WEB_API))
			.addServiceSupport(ServiceType.EXTERNAL, Actor.getInstance(CONVERTER))
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
		
		// EXTERNAL: Default route / Payload response
		Future<Response> future = executors.submit(() -> {
			
//			Map<String, String> requestTestMsg = Map.of(
//					"parameters", 
//						"{" + 
//							"\"DCAT-ID\": \"NICELONGBLAHBLAHTHINGYID001\", " +  
//							"\"response-content-type\": \"application/epos.geo+json\" " + 
//						"}",
//					"content", 
//						"<?xml version='1.0' encoding='UTF-8' standalone='no'?>" + 
//						"<FDSNStationXML xmlns='http://www.fdsn.org/xml/station/1' schemaVersion='1.0'>" +
//						"	<Source>Squiffy</Source>" +	
//						"	<Created>2019-02-20T13:27:42</Created>" + 
//						"	<Network code='NETWORK NAME #1'>" +
//						"		<Description>SOME COMMENT #1</Description>" +
//						"		<Station code='SOME MARKER #1'>" +
//						"			<Latitude>50.8506</Latitude>" +
//						"			<Longitude>111.12345678</Longitude>" +
//						"			<Elevation>1030.01</Elevation>" +
//						"			<Site>" +
//						"				<Name>SOME CITY NAME #1</Name>" +
//						"				<Country>SOME COUNTRY NAME #1</Country>" +
//						"			</Site>" +
//						"			<CreationDate>2019-02-20T13:27:41</CreationDate>" +
//						"		</Station>" +
//						"	</Network>" +
//						"</FDSNStationXML>"
//				);
			
		    FileInputStream fis = new FileInputStream("src/test/resources/stringtoolong.txt");
		    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

		    StringBuilder out = new StringBuilder();
		    String line;
		    while ((line = reader.readLine()) != null) {
		        out.append(line);
		    }

		    String msgPayload = out.toString();   
				
			return _router.makeRequest(RequestBuilder.instance(ServiceType.EXTERNAL, "get", "execute")
						.addHeaders(null)
						//.addPayloadPropertiesMap((Map<String, String> & Serializable) requestTestMsg)
						.addPayloadPlainText(msgPayload)
						.build());
		});
		
		try {
			Response response = future.get();
			
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
	    						"===============================%n",
	    						response.getComponentAudit(),
	    						response.getPayloadAsPlainText().get());
	    				LOG.debug(responseReport);
	    				System.out.println(responseReport);
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
	    						"===============================%n",
	    						response.getComponentAudit(),
	    						mapAsStr);
	    				System.out.println(responseReport);
						LOG.debug(responseReport);
						break;
					}
					default:
						break;	    
				}
			}
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

		
	}
	
}
