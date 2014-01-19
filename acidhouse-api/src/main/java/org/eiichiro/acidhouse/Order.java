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

import java.util.Comparator;

import org.eiichiro.acidhouse.metamodel.ComparableProperty;
import org.eiichiro.acidhouse.metamodel.EmbeddedProperty;

/**
 * {@code Order} represents a sorting order for Acid Houses' typesafe "Command 
 * builder API".
 * You can get {@code Order} instance from metamodel property and can pass to 
 * {@code GetList} and {@code GetScalar}. As an example: 
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
 * List&lt;Entity3&gt; entity3s = session.get(entity3_)
 * 		.sort(entity3_.entity1.i.desc, entity3_.i.desc)
 * 		.execute();
 * </pre>
 * 
 * @see ComparableProperty
 * @see GetList
 * @see GetScalar
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Order<E, T extends Comparable<T>> implements Comparator<E> {

	/**
	 * {@code Direction} represents a sorting direction.
	 * 
	 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
	 */
	public static enum Direction {

		/** Descending order. */
		DESC, 
		
		/** Ascending order. */
		ASC
		
	}
	
	private final ComparableProperty<E, T> property;
	
	private final Order.Direction direction;
	
	/**
	 * Constructs a new {@code Order} instance with the specified 
	 * {@code ComparableProperty} and sorting direction.
	 * 
	 * @param property {@code ComparableProperty} to be sorted.
	 * @param direction The sorting direction.
	 */
	public Order(ComparableProperty<E, T> property, Order.Direction direction) {
		this.property = property;
		this.direction = direction;
	}

	/**
	 * Returns {@code ComparableProperty} to be sorted.
	 * 
	 * @return The {@code ComparableProperty} to be sorted.
	 */
	public ComparableProperty<E, T> property() {
		return property;
	}

	/**
	 * Returns sorting direction.
	 * 
	 * @return The sorting direction.
	 */
	public Order.Direction direction() {
		return direction;
	}

	/**
	 * Compares the specified two entities.
	 * 
	 * @param entity1 An entity to be compared.
	 * @param entity2 An entity to be compared.
	 * @return The comparison result.
	 */
	@Override
	public int compare(E entity1, E entity2) {
		T value = property.get(entity1);
		T value2 = property.get(entity2);
		return value.compareTo(value2);
	}
	
	/**
	 * Returns the custom string representation.
	 * 
	 * @return The custom string representation.
	 */
	@Override
	public String toString() {
		String name = (property.parent() instanceof EmbeddedProperty) 
				? property.parent().name() + "." + property.name() : property.name();
		return name + " " + direction;
	}
	
}
