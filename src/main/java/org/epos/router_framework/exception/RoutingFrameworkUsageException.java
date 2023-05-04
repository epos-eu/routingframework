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
 * Indicates a problem with the client's usage of the framework; either due to initial {@link org.epos.router_framework.Router} configuration
 * or with the implementation of {@link org.epos.router_framework.handling.RelayRouterHandler}
 *
 */
public class RoutingFrameworkUsageException extends RoutingException {
	
	private static final long serialVersionUID = -3201797604281518822L;
	
	public RoutingFrameworkUsageException(String errMsg) {
		super(errMsg);
	}

}
