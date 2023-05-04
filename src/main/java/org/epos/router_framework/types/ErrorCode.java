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
package org.epos.router_framework.types;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

public enum ErrorCode {
	USAGE_ERROR("usage-error"),				// logic error with handler implementation
	HANDLER_ERROR("handling-error"),		// error when handler implementation is processing messages (e.g. due to getting a HTTP 503 back) 
	INTERNAL_ERROR("internal-error"),		// Network error e.g. RabbitMQ communication
	TIMED_OUT("timed-out"),
	LOGIC_ERROR("logic-error");				// logic error with core code
	
	private String label;
	
	private ErrorCode(String label) {
		this.label = label;
	}
	
	public static Optional<ErrorCode> getInstance(String value) {
		return StringUtils.isBlank(value) ? Optional.empty() : 
			Arrays.stream(ErrorCode.values())
			.filter(v -> !Objects.isNull(v))
			.filter(v -> StringUtils.isNotBlank(v.label) && v.label.equals(value))
			.findAny();
	}

	public String getLabel() {
		return label;
	}
	
}
