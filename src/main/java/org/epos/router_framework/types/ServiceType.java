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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * Each instance should end up being represented by a Topic Exchange
 */
public enum ServiceType {
	
	METADATA("metadataService"),
	WORKSPACE("workspaceService"),
	EXTERNAL("externalAccess"),
	INGESTOR("ingestorService"),
	PROCESS("processService"),
	SCHEDULER("schedulerService"),
	CONVERTER("converterService"),
	SENDER("senderService");
	
	private String serviceLabel;
	
	private ServiceType(String serviceLabel) {
		this.serviceLabel = serviceLabel;		
	}
	
	public String getServiceLabel() {
		return serviceLabel;
	}

	public static Optional<ServiceType> getInstance(String value) 
	{
		return StringUtils.isBlank(value) ? Optional.empty() : 
					Arrays.stream(ServiceType.values())
						.filter(v -> !Objects.isNull(v))
						.filter(v -> StringUtils.isNotBlank(v.getServiceLabel()) && v.getServiceLabel().equals(value))
						.findAny();
	}
	
	public static String toString(Optional<Set<ServiceType>> servicesTypesOpt) 
	{
		Set<ServiceType> servicesTypes = servicesTypesOpt.isEmpty() ? Set.of() : servicesTypesOpt.get();

		return servicesTypes.stream()
			.map(ServiceType::getServiceLabel)
			.collect(Collectors.joining("; ", "[", "]"));
	}
	
}
