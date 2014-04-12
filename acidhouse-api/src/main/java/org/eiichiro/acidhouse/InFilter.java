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

import java.util.Arrays;
import java.util.List;

import org.eiichiro.acidhouse.ComparableFilter.Operator;
import org.eiichiro.acidhouse.metamodel.ComparableProperty;
import org.eiichiro.acidhouse.metamodel.EmbeddedProperty;
import org.eiichiro.acidhouse.metamodel.Property;

/**
 * {@code InFilter} is a {@code Filter} implementation to do filtering the 
 * property which matches to any of the specified values (This is well-known as 
 * SQL 'IN' predicate).
 * You can get this instance from {@code ComparableProperty#in(Comparable...)} 
 * method.
 * 
 * @see ComparableProperty
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class InFilter<T extends Comparable<T>> implements Filter<T> {

	private final Property<?, T> property;
	
	private final List<T> values;
	
	/**
	 * Constructs a new {@code InFilter} instance with the specified 
	 * {@code ComparableProperty} instance and values.
	 * 
	 * @param property The metamodel property this filter targets.
	 * @param values The values this filter indicates.
	 */
	@SafeVarargs
	public InFilter(ComparableProperty<?, T> property, T... values) {
		if (property == null) {
			throw new IllegalArgumentException("'property' must not be [" + property + "]");
		}
		
		if (values == null) {
			throw new IllegalArgumentException("'values' must not be [" + values + "]");
		}
		
		if (values.length == 0) {
			throw new IllegalArgumentException("Length of 'values' must be greater than [" + values.length + "]");
		}
		
		this.property = property;
		this.values = Arrays.asList(values);
	}
	
	/**
	 * Constructs a new {@code InFilter} instance with the specified 
	 * {@code ComparableProperty} instance and values.
	 * 
	 * @param property The metamodel property this filter targets.
	 * @param values The values this filter indicates.
	 */
	public InFilter(ComparableProperty<?, T> property, List<T> values) {
		if (property == null) {
			throw new IllegalArgumentException("'property' must not be [" + property + "]");
		}
		
		if (values == null) {
			throw new IllegalArgumentException("'values' must not be [" + values + "]");
		}
		
		if (values.isEmpty()) {
			throw new IllegalArgumentException("Size of 'values' must be greater than [" + values.size() + "]");
		}
		
		this.property = property;
		this.values = values;
	}
	
	/**
	 * Returns the property that this filter targets.
	 * 
	 * @return The property that this filter targets.
	 */
	@Override
	public Property<?, T> property() {
		return property;
	}
	
	/**
	 * Indicates whether the specified entity's property matches to this 
	 * {@code InFilter} or not.
	 * 
	 * @param entity The entity to be filtered.
	 * @return Whether the specified entity matches to this {@code InFilter} or 
	 * not.
	 */
	@Override
	public boolean matches(Object entity) {
		T t = property.get(entity);
		
		for (T value : values) {
			if (t.compareTo(value) == 0) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Returns values to be tested.
	 * 
	 * @return The values to be tested.
	 */
	public List<T> values() {
		return values;
	}
	
	/**
	 * Returns the {@code String} representation of this instance.
	 * 
	 * @return The {@code String} representation of this instance.
	 */
	@Override
	public String toString() {
		String name = (property.parent() instanceof EmbeddedProperty) 
				? property.parent().name() + "." + property.name() : property.name();
		StringBuilder builder = new StringBuilder("(");
		builder.append(name + " " + Operator.EQUAL_TO + " "
				+ toFilterString(values.get(0)));
		
		for (int i = 1; i < values.size(); i++) {
			builder.append(" || " + name + " " + Operator.EQUAL_TO + " " + toFilterString(values.get(i)));
		}
		
		return builder.append(")").toString();
	}
	
	private String toFilterString(final Object object) {
		return (object instanceof String) ? "\"" + object + "\"" : object.toString();
	}
	
}
