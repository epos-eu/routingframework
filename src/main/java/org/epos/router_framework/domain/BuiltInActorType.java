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
package org.epos.router_framework.domain;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the known actor type at compile-time
 */
public enum BuiltInActorType {
	BACK_OFFICE("admin"),
	CONVERTER("map"),
	QUERY_GENERATOR("query"),
	WORKSPACE("workspace"),
	VALIDATOR("validate"),
	TCS_CONNECTOR("access"),
	DB_CONNECTOR("fetch"),
	INGESTOR("ingest"),
	EMAIL_SENDER("sender"),
	WEB_API("web-api");

	private static final Map<String, BuiltInActorType> ACTORS = Arrays.stream(BuiltInActorType.values())
			.collect(Collectors.toMap(BuiltInActorType::verbLabel, Function.identity()));
	
	// the verb label value should also be unique to each BuiltInActorType instance
	private String verbLabel;
	
	private BuiltInActorType(String verbLabel) {
		this.verbLabel = verbLabel;
	}
	
	public static Optional<BuiltInActorType> instance(String value) {
		return Optional.ofNullable(ACTORS.get(value));
	}
	
	public String verbLabel() {
		return verbLabel;
	}

}