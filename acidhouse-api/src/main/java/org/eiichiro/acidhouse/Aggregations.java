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

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;

import org.eiichiro.acidhouse.metamodel.Property;

/**
 * {@code Aggregations} has some factory methods to provide useful 
 * {@code Aggregation} instances.
 * You can get popular aggregations by using this class. For example: 
 * <pre>
 * import org.eiichiro.acidhouse.Aggregations;
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.metamodel.Metamodels;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Create 'Session' instance.
 * Session session = new AppEngineDatastoreSession();
 * // Get metamodel instance of 'Entity3' class.
 * Entity3_ entity3_ = Metamodels.metamodel(Entity3.class);
 * // Get max value of "Entity3"'s 'i' property from 'Entity3' instances which 
 * // their 'i' property value is less than 5.
 * int max = session
 * 		.get(Aggregations.<b>max</b>(entity3_.i))
 * 		.filter(entity3_.i.lessThan(5))
 * 		.execute();
 * </pre>
 * 
 * <b>Better to static-import this class.</b>
 * 
 * @see Aggregation
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public abstract class Aggregations {

	private Aggregations() {}
	
	/**
	 * Get the max aggregation implementation for the specified property.
	 * 
	 * @param <T> The property value type that this max aggregation aggregates.
	 * @param property The metamodel property that this max aggregation 
	 * aggregates.
	 * @return Max aggregation implementation for the specified property.
	 */
	public static <T extends Comparable<T>> Aggregation<T> max(Property<?, T> property) {
		return new Max<T>(property);
	}
	
	private static class Max<T extends Comparable<T>> extends Aggregation<T> {

		public Max(Property<?, T> property) {
			super(property);
		}
		
		@Override
		public T aggregate(List<T> list) {
			if (list.size() == 0) {
				return null;
			}
			
			Iterator<T> iterator = list.iterator();
			T max = iterator.next();
			
			while (iterator.hasNext()) {
				T next = iterator.next();
				
				if (next.compareTo(max) > 0) {
					max = next;
				}
			}
			
			return max;
		}
		
	}
	
	/**
	 * Get the min aggregation implementation for the specified property.
	 * 
	 * @param <T> The property value type that this min aggregation aggregates.
	 * @param property The metamodel property that this min aggregation 
	 * aggregates.
	 * @return Min aggregation implementation for the specified property.
	 */
	public static <T extends Comparable<T>> Aggregation<T> min(Property<?, T> property) {
		return new Min<T>(property);
	}
	
	private static class Min<T extends Comparable<T>> extends Aggregation<T> {

		public Min(Property<?, T> property) {
			super(property);
		}
		
		@Override
		public T aggregate(List<T> list) {
			if (list.size() == 0) {
				return null;
			}
			
			Iterator<T> iterator = list.iterator();
			T min = iterator.next();
			
			while (iterator.hasNext()) {
				T next = iterator.next();
				
				if (next.compareTo(min) < 0) {
					min = next;
				}
			}
			
			return min;
		}
		
	}
	
	/**
	 * Get the sum aggregation implementation for the specified property.
	 * 
	 * @param <T> The property value type that this sum aggregation aggregates.
	 * @param property The metamodel property that this sum aggregation 
	 * aggregates.
	 * @return Sum aggregation implementation for the specified property.
	 */
	public static <T extends Number & Comparable<T>> Aggregation<T> sum(Property<?, T> property) {
		return new Sum<T>(property);
	}
	
	private static class Sum<T extends Number & Comparable<T>> extends Aggregation<T> {

		private final Type type;
		
		public Sum(Property<?, T> property) {
			super(property);
			type = property.type();
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public T aggregate(List<T> list) {
			int i = 0;
			long l = 0;
			double d = 0;
			short s = 0;
			float f = 0;
			
			for (T number : list) {
				if (type.equals(Integer.class)) {
					i += number.intValue();
				} else if (type.equals(Long.class)) {
					l += number.longValue();
				} else if (type.equals(Double.class)) {
					d += number.doubleValue();
				} else if (type.equals(Short.class)) {
					s += number.shortValue();
				} else {
					f += number.floatValue();
				}
			}
			
			Object object;
			
			if (type.equals(Integer.class)) {
				object = Integer.valueOf(i);
			} else if (type.equals(Long.class)) {
				object = Long.valueOf(l);
			} else if (type.equals(Double.class)) {
				object = Double.valueOf(d);
			} else if (type.equals(Short.class)) {
				object = Short.valueOf(s);
			} else {
				object = Float.valueOf(f);
			}
			
			return (T) object;
		}
		
	}
	
}
