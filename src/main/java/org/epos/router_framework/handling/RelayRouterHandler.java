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
package org.epos.router_framework.handling;

import java.util.Objects;
import java.util.Optional;

import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.types.PayloadType;

public abstract class RelayRouterHandler {
	
	private final Actor defaultNextActor;
	private Optional<Actor> overiddenNextActor = Optional.empty();
	private PayloadType nextPayloadType;
	
	public RelayRouterHandler(Actor defaultNextActor, PayloadType nextPayloadType) {
		this.defaultNextActor = Objects.requireNonNull(defaultNextActor);
		this.nextPayloadType = Objects.requireNonNull(nextPayloadType);
	}
	
	public PayloadType getNextPayloadType() {
		return nextPayloadType;
	}
	
	public Actor getDefaultNextActor() {
		return defaultNextActor;
	}
	
	public Optional<Actor> getOverriddenNextActor() {
		return overiddenNextActor;
	}
	
	protected void setNextPayloadType(PayloadType nextPayloadType) {
		this.nextPayloadType = nextPayloadType;
	}
	
	protected void setOverriddenNextActor(Actor nextActor) {
		this.overiddenNextActor = Optional.ofNullable(nextActor);
	}
	

	

}
