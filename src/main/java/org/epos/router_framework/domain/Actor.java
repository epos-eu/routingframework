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
 * Represents components within the ICS-C architecture with respect to inter-component communication
 * Permits runtime additions to the available actors.
 */
public final class Actor
{	
	private String label;
	private String name;
	
	private static final Map<String, Actor> ACTORS = Arrays.stream(BuiltInActorType.values())
			.map(Actor::new)
			.collect(Collectors.toMap(Actor::verbLabel, Function.identity()));
	
	public static Optional<Actor> getInstance(String actorLabel) {
		return Optional.ofNullable(ACTORS.get(actorLabel));
	}
	
	public static Actor getInstance(BuiltInActorType actor) {
		return ACTORS.get(actor.verbLabel());
	}
	
	public static Actor newInstance(String actorLabel, String actorName) {		
		return new Actor(actorLabel, actorName);
	}
	
	private Actor(BuiltInActorType actorType) {
		this.label = actorType.verbLabel();
		this.name = actorType.name();
	}
	
	private Actor(String label, String name) {
		this.label = label;
		this.name = name;
		ACTORS.put(label, this);
	}
	
	public String verbLabel() {
		return label;
	}
	
	public String verbLabelReturnFlavour() {
		return String.format("%s_return", label);
	}
	
	public String name() {
		return name;
	}
	
}

