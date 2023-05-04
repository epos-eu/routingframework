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
package org.epos.router_framework.exception;

/**
 * Indicates a handling exception occurring within a {@link org.epos.router_framework.handling.RelayRouterHandler} that the 
 * handler implementation has deemed it cannot recover from.
 * 
 * <p><em>Examples could include -</em></p>
 * <ul>
 * 	 <li>Handler implementation attempted required access to an external service but could not establish a connection</li>
 *   <li>The format/structure of an incoming message was not expected and could not be handled; no meaningful response could therefore be generated</li>
 * </ul>
 */
public class RoutingMessageHandlingException extends RoutingException {

	private static final long serialVersionUID = 1L;
	
	public RoutingMessageHandlingException(String message) {
		super(message);
	}

	public RoutingMessageHandlingException(String message, Exception e) {
		super(message, e);
	}
}
