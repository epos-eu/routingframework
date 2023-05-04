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

public enum PayloadType {
	
	PLAIN_TEXT("plain-text"),
	PROPERTIES_MAP("properties-map"),
	EMPTY("no-payload");
	
	private String label;

	private PayloadType(String label) {
		this.label = label;
	}
	
	public String getLabel() {
		return label;
	}
	
	public static Optional<PayloadType> getInstance(String value) {
		return StringUtils.isBlank(value) ? Optional.empty() : 
			Arrays.stream(PayloadType.values())
			.filter(v -> !Objects.isNull(v))
			.filter(v -> StringUtils.isNotBlank(v.label) && v.label.equals(value))
			.findAny();
	}
}
