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

import org.eiichiro.acidhouse.metamodel.ComparableProperty;
import org.eiichiro.acidhouse.metamodel.EmbeddedProperty;
import org.eiichiro.acidhouse.metamodel.Property;

/**
 * {@code ComparableFilter} is a {@code Filter} implementation to do filtering 
 * comparable property values (Make sure these property value types implements 
 * {@code Comparable}).
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class ComparableFilter<T extends Comparable<T>> implements Filter<T> {

	/**
	 * {@code Operator} represents the type of property comparison operator.
	 * 
	 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
	 */
	public static enum Operator {
		
		/** The equal (==) operator. */
		EQUAL_TO("==") {

			@Override
			public boolean matches(int compareTo) {
				return compareTo == 0;
			}
			
		}, 
		
		/** The not-equal (!=, &lt;&gt;) operator. */
		NOT_EQUAL_TO("!=") {

			@Override
			public boolean matches(int compareTo) {
				return compareTo != 0;
			}
			
		}, 
		
		/** The greater-than (&gt;) operator. */
		GREATER_THAN(">") {

			@Override
			public boolean matches(int compareTo) {
				return compareTo > 0;
			}
			
		}, 
		
		/** The greater-than-or-equal (&gt;=) operator. */
		GREATER_THAN_OR_EQUAL_TO(">=") {

			@Override
			public boolean matches(int compareTo) {
				return compareTo >= 0;
			}
			
		}, 
		
		/** The less-than (&lt;) operator. */
		LESS_THAN("<") {

			@Override
			public boolean matches(int compareTo) {
				return compareTo < 0;
			}
			
		}, 
		
		/** The less-than-or-equal-to (&lt;=) operator. */
		LESS_THAN_OR_EQUAL_TO("<=") {

			@Override
			public boolean matches(int compareTo) {
				return compareTo <= 0;
			}
			
		};
		
		private Operator(String expression) {
			this.expression = expression;
		}
		
		private final String expression;
		
		/**
		 * Indicates whether the specified {@code Comparable#compareTo(Object)} 
		 * result matches to this operator or not. {@code Comparable#compareTo(Object)} 
		 * is called by {@code ComparableFilter} before this method is called.
		 * 
		 * @param compareTo {@code Comparable#compareTo(Object)} result.
		 * @return Whether this operator matches to the specified 
		 * {@code Comparable#compareTo(Object)} result or not.
		 */
		abstract boolean matches(int compareTo);

		/** Returns the {@code String} representation of this operator. */
		@Override
		public String toString() {
			return expression;
		}
		
	}
	
	private final Property<?, T> property;
	
	private final T value;
	
	private final Operator operator;
	
	/**
	 * Constructs a new {@code ComparableFilter} instance with the specified 
	 * {@code ComparableProperty}, property value and operator type.
	 * This constructor is called by {@code ComparableProperty}'s filtering 
	 * methods.
	 * 
	 * @param property The {@code ComparableProperty} instance
	 * @param value The property value
	 * @param operator The operator type.
	 */
	public ComparableFilter(ComparableProperty<?, T> property, T value, Operator operator) {
		if (property == null) {
			throw new IllegalArgumentException("'property' must not be [" + property + "]");
		}
		
		this.property = property;
		this.value = value;
		this.operator = operator;
	}
	
	/** Returns the property that this {@code ComparableFilter} does filtering. */
	@Override
	public Property<?, T> property() {
		return property;
	}
	
	/**
	 * Indicates the specified entity instance matches to this 
	 * {@code ComparableFilter}. 
	 */
	@Override
	public boolean matches(Object entity) {
		T left = property.get(entity);
		return operator().matches(left.compareTo(value()));
	}
	
	/** Returns the {@code String} representation of this instance. */
	@Override
	public String toString() {
		Object value = this.value();
		String string = (value instanceof String) ? "\"" + value + "\"" 
				: value.toString();
		String name = (property.parent() instanceof EmbeddedProperty) 
				? property.parent().name() + "." + property.name() : property.name();
		return name + " " + operator() + " " + string;
	}

	/** Returns the operator type this {@code ComparableFilter} does filtering. */
	public Operator operator() {
		return operator;
	}

	/** Returns the value this {@code ComparableFilter} does filtering. */
	public T value() {
		return value;
	}
	
}
