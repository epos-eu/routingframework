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
package org.epos.router_framework.util;

import java.util.function.Function;
import java.util.function.Predicate;

import lombok.Lombok;

public class ExceptionUtils {
	
	private ExceptionUtils() {
		throw new IllegalStateException("Utility class");
	}
	
	public static <T, U extends Exception> void raiseIssueIf(T obj, Predicate<T> p, Function<String, U> exFunc, String errMsg) {
		if (p.test(obj)) {
			throw Lombok.sneakyThrow(exFunc.apply(errMsg));
		}
	}
			  
	/**
	 * @param <T> Type of object of interest
	 * @param obj object of interest
	 * @param exceptionMsg message to pass to the {@link IllegalArgumentException}
	 * @param predicates tests to be carried out against the object of interest
	 * @return object of interest
	 * @throws IllegalArgumentException thrown if any of the tests against the object of interest fail
	 */
	@SafeVarargs
	public static <T> T requireConditionOrElseThrowIAE(T obj, String exceptionMsg, Predicate<T>... predicates)
	{
		Predicate<T> combinedPredicate = p -> Boolean.TRUE;
		
		for (Predicate<T> predicate : predicates) {
			combinedPredicate.and(predicate);
		}
		
		if (!combinedPredicate.test(obj)) {
			throw new IllegalArgumentException(exceptionMsg);
		}
		
		return obj;
	}
		
}
