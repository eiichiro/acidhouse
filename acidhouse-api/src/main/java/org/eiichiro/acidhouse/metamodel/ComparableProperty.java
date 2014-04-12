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
package org.eiichiro.acidhouse.metamodel;

import java.lang.reflect.Type;

import org.eiichiro.acidhouse.ComparableFilter;
import org.eiichiro.acidhouse.ComparableFilter.Operator;
import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.InFilter;
import org.eiichiro.acidhouse.Order;

/**
 * {@code ComparableProperty} is a {@code Property} extension represents a 
 * comparable property.
 * You can get several {@code ComparableFilter} instances from this class, and 
 * then pass them to command's {@code #filter(Filter...)} methods, like this: 
 * <pre>
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Create 'Session' instance.
 * Session session = new AppEngineDatastoreSession();
 * // Get metamodel instance of 'Entity3' class.
 * Entity3_ entity3_ = Metamodels.metamodel(Entity3.class);
 * // Qualify entities which their 'entity1.i' proerty is greater than or equal 
 * // to 13, and 15 or 16.
 * List&lt;Entity3&gt; entity3s = session
 * 		.get(Entity3.class)
 * 		.filter(entity3_.entity1.i.greaterThanOrEqualTo(13), 
 * 			entity3_.entity1.i.in(15, 16))
 * 		.execute();
 * </pre>
 * And sorting order. Like this: 
 * <pre>
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Create 'Session' instance.
 * Session session = new AppEngineDatastoreSession();
 * // Get metamodel instance of 'Entity3' class.
 * Entity3_ entity3_ = Metamodels.metamodel(Entity3.class);
 * // Get all 'Entity3' entities sorted by 'entity1.i' property in descending 
 * // order and 'i' property in descending order.
 * List&lt;Entity3&gt; entity3s = session.get(Entity3.class)
 * 		.sort(entity3_.entity1.i.desc, entity3_.i.asc)
 * 		.execute();
 * </pre>
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class ComparableProperty<E, T extends Comparable<T>> extends Property<E, T> {

	/** Sorting order: descending direction. */
	public final Order<E, T> desc;
	
	/** Sorting order: ascending direction. */
	public final Order<E, T> asc;
	
	/**
	 * Constructs a new {@code ComparableProperty} instance with the specified 
	 * {@code Metamodel} instance which has this metamodel property, property 
	 * type and property name.
	 * 
	 * @param metamodel The {@code Metamodel} instance which has this metamodel 
	 * property.
	 * @param type The property type.
	 * @param name The property name.
	 */
	public ComparableProperty(Metamodel<E> metamodel, Type type, String name) {
		super(metamodel, type, name);
		desc = new Order<E, T>(this, Order.Direction.DESC);
		asc = new Order<E, T>(this, Order.Direction.ASC);
	}

	/**
	 * Constructs a new {@code ComparableProperty} instance with the specified 
	 * {@code Property} instance which has this metamodel property, property 
	 * type and property name.
	 * 
	 * @param parent The parent {@code Property} instance which has this metamodel 
	 * property.
	 * @param type The property type.
	 * @param name The property name.
	 */
	public ComparableProperty(Property<E, ?> parent, Type type, String name) {
		super(parent, type, name);
		desc = new Order<E, T>(this, Order.Direction.DESC);
		asc = new Order<E, T>(this, Order.Direction.ASC);
	}
	
	/**
	 * Creates a {@code Filter} to indicate whether this property is equal to 
	 * the specified value or not (==).
	 * 
	 * @param value The value to be evaluated.
	 * @return The {@code Filter} to indicate whether this property is equal to 
	 * the specified value or not.
	 */
	public Filter<T> equalTo(T value) {
		return new ComparableFilter<T>(this, value, Operator.EQUAL_TO);
	}
	
	/**
	 * Creates a {@code Filter} to indicate whether this property is not equal 
	 * to the specified value or not (!=, &lt;&gt;).
	 * 
	 * @param value The value to be evaluated.
	 * @return The {@code Filter} to indicate whether this property is not equal 
	 * to the specified value or not.
	 */
	public Filter<T> notEqualTo(T value) {
		return new ComparableFilter<T>(this, value, Operator.NOT_EQUAL_TO);
	}
	
	/**
	 * Creates a {@code Filter} to indicate whether this property is greater 
	 * than the specified value or not (&gt;).
	 * 
	 * @param value The value to be evaluated.
	 * @return The {@code Filter} to indicate whether this property is greater 
	 * than the specified value or not.
	 */
	public Filter<T> greaterThan(T value) {
		return new ComparableFilter<T>(this, value, Operator.GREATER_THAN);
	}
	
	/**
	 * Creates a {@code Filter} to indicate whether this property is greater 
	 * than or equal to the specified value or not (&gt;=).
	 * 
	 * @param value The value to be evaluated.
	 * @return The {@code Filter} to indicate whether this property is greater 
	 * than or equal to the specified value or not.
	 */
	public Filter<T> greaterThanOrEqualTo(T value) {
		return new ComparableFilter<T>(this, value, Operator.GREATER_THAN_OR_EQUAL_TO);
	}
	
	/**
	 * Creates a {@code Filter} to indicate whether this property is less than 
	 * the specified value or not (&lt;).
	 * 
	 * @param value The value to be evaluated.
	 * @return The {@code Filter} to indicate whether this property is less than 
	 * the specified value or not.
	 */
	public Filter<T> lessThan(T value) {
		return new ComparableFilter<T>(this, value, Operator.LESS_THAN);
	}
	
	/**
	 * Creates a {@code Filter} to indicate whether this property is less than 
	 * or equal to the specified value or not (&lt;=).
	 * 
	 * @param value The value to be evaluated.
	 * @return The {@code Filter} to indicate whether this property is less than 
	 * or equal to the specified value or not.
	 */
	public Filter<T> lessThanOrEqualTo(T value) {
		return new ComparableFilter<T>(this, value, Operator.LESS_THAN_OR_EQUAL_TO);
	}

	/**
	 * Creates a {@code Filter} to indicate whether this property is contained 
	 * in the specified value or not (IN(...)).
	 * 
	 * @param value The value to be evaluated.
	 * @return The {@code Filter} to indicate whether this property is contained 
	 * in the specified value or not.
	 */
	@SuppressWarnings("unchecked")
	public Filter<T> in(T... values) {
		return new InFilter<T>(this, values);
	}
	
}
