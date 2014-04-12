/*
 * Copyright (C) 2011 Eiichiro Uchiumi. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eiichiro.acidhouse;

import java.util.List;

import org.eiichiro.acidhouse.metamodel.Property;

/**
 * {@code Aggregation} is the base class of aggregation implementation.
 * You can aggregate one property of the <code>GetList</code> command result, 
 * by specifying this classes' concrete instance to 
 * {@link Session#get(Aggregation)} method.
 * Class {@code Aggregations} has some factory methods to provide useful 
 * {@code Aggregation} instances.
 * 
 * @see Aggregations
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public abstract class Aggregation<T> {

	private final Property<?, T> property;
	
	/**
	 * Constructs a new {@code Aggregation} instance for the specified property.
	 * 
	 * @param property The property to aggregate.
	 */
	public Aggregation(final Property<?, T> property) {
		if (property == null) {
			throw new IllegalArgumentException("'property' must not be [" + property + "]");
		}
		
		this.property = property;
	}
	
	/**
	 * Aggregates the specified list and returns the aggregation result.
	 * This method is called back by the {@code GetScalar} implementation. 
	 * The specified list's entry is the value of the property which this 
	 * aggregation instance has.
	 * 
	 * @param list The value list of the property which this aggregation 
	 * instance attempt to aggregate.
	 * @return The aggregation result as a scalar value.
	 */
	public abstract T aggregate(List<T> list);

	/**
	 * Returns the property that this aggregation instance aggregates.
	 * 
	 * @return The property that this aggregation instance aggregates.
	 */
	public Property<?, T> property() {
		return property;
	}
	
}
