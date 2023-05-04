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

import org.apache.commons.lang3.StringUtils;

public class RoutingException extends Exception {
	
	private static final long serialVersionUID = 2725149619329081795L;

	public RoutingException(String message) {
		super(message);
	}
	
	public RoutingException(String message, Exception e) {
		super(message, e);
	}	
	
	public static RoutingException channelNotObtained() {
		return channelNotObtained("");
	}
	
	public static RoutingException channelNotObtained(String errMsg) {
		String msg = getChannelNotObtainedMsg(errMsg);	
		return new RoutingException(msg);
	}
	
	public static RoutingException channelNotObtained(Exception e) {
		return channelNotObtained("", e);
	}
	
	public static RoutingException channelNotObtained(String errMsg, Exception e) {
		String msg = getChannelNotObtainedMsg(errMsg);
		return new RoutingException(msg, e);
	}

	private static String getChannelNotObtainedMsg(String errMsg) {	
		final String msgPrefix = "Could not obtain channel";
		
		if (StringUtils.isBlank(errMsg)) {
			return String.format("%s%n", msgPrefix);
		} else {
			return String.format("%s: %s%n", msgPrefix, errMsg);
		}
	}
	
	public static RoutingException failedToPublish(Exception e) {
		return new RoutingException("Could not publish to channel", e);
	}
	
	public static RoutingException failedToConsume(Exception e) {
		return new RoutingException("Could not consume from channel", e);
	}

	public static RoutingException connectionNotObtained(Exception e) {
		return new RoutingException("Could not obtain connection", e);
	}

	public static RoutingException amqpEntityConfigIssue(String errMsg, Exception e) {
		return new RoutingException("AMQP Entity configuration issue: " + errMsg, e);
	}

}
