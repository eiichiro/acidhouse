/*
 * Copyright (C) 2011-2012 Eiichiro Uchiumi. All Rights Reserved.
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
package org.eiichiro.acidhouse.appengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.ComparableFilter;
import org.eiichiro.acidhouse.Entities;
import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.GetList;
import org.eiichiro.acidhouse.InFilter;
import org.eiichiro.acidhouse.Order;
import org.eiichiro.acidhouse.metamodel.EmbeddedProperty;
import org.eiichiro.acidhouse.metamodel.Metamodel;
import org.eiichiro.acidhouse.metamodel.Property;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;

/**
 * {@code AppEngineGetList} is a App Engine Low-level Datastore API based 
 * implementation of {@code GetList}.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineGetList<E> implements GetList<E> {

	private Logger logger = Logger.getLogger(getClass().getName());
	
	private final Metamodel<E> metamodel;
	
	private final AppEngineDatastoreSession session;
	
	private List<Filter<?>> filters = new ArrayList<Filter<?>>(0);
	
	private int limit = Integer.MAX_VALUE;
	
	private int offset = 0;
	
	private List<Order<?, ?>> orders = new ArrayList<Order<?, ?>>(0);
	
	/**
	 * Constructs a new {@code AppEngineGetList} with the specified metamodel of 
	 * entity and {@code AppEngineDatastoreSession}.
	 * 
	 * @param metamodel The metamodel of entity to get with this command.
	 * @param session {@code AppEngineDatastoreSession} instance for this command.
	 */
	public AppEngineGetList(Metamodel<E> metamodel, AppEngineDatastoreSession session) {
		this.metamodel = metamodel;
		this.session = session;
	}
	
	/**
	 * Executes {@code GetList} with {@code AppEngineDatastoreSession}.
	 * 
	 * @return The entities match to the specified {@code Filter}s in the 
	 * specified range ordered by the specified sort orders as a {@code List} 
	 * view.
	 */
	@Override
	public List<E> execute() {
		logger.fine("Executing [AppEngineGetList] command");
		Class<E> type = metamodel.type();
		Query query = new Query(Translation.toKind(type));
		QueryRestriction restriction = new QueryRestriction();
		Set<Filter<?>> subfilters = new HashSet<Filter<?>>();
		
		for (Filter<?> filter : filters) {
			if (restriction.restricted(filter)) {
				subfilters.add(filter);
			} else {
				Property<?, ?> property = filter.property();
				String name = (property.parent() instanceof EmbeddedProperty) 
						? property.parent().name() + "." + property.name() : property.name();
				boolean key = false;
				
				if (name.equals(Entities.keyField(type).getName())) {
					name = Entity.KEY_RESERVED_PROPERTY;
					key = true;
				}
				
				if (filter instanceof ComparableFilter<?>) {
					ComparableFilter<?> comparableFilter = (ComparableFilter<?>) filter;
					Object value = comparableFilter.value();
					
					if (key) {
						value = Keys.create(Translation.toKind(type), value);
					}
					
					query.setFilter(new Query.FilterPredicate(
							name, Translation.toFilterOperator(comparableFilter.operator()), value));
				} else if (filter instanceof InFilter<?>) {
					InFilter<?> inFilter = (InFilter<?>) filter;
					List<?> values = inFilter.values();
					
					if (key) {
						List<Object> list = new ArrayList<Object>();
						
						for (Object value : values) {
							list.add(Keys.create(Translation.toKind(type), value));
						}
						
						values = list;
					}
					
					query.setFilter(new Query.FilterPredicate(name, FilterOperator.IN, values));
				}
			}
		}
		
		final List<Order<?, ?>> suborders = new ArrayList<Order<?, ?>>();
		
		for (Order<?, ?> order : orders) {
			if (restriction.restricted(order)) {
				suborders.add(order);
			} else {
				Property<?, ?> property = order.property();
				String name = (property.parent() instanceof EmbeddedProperty) 
						? property.parent().name() + "." + property.name() : property.name();
				query.addSort(name, Translation.toSortDirection(order.direction()));
			}
		}
		
		FetchOptions options = FetchOptions.Builder.withOffset(0);
		
		if (subfilters.isEmpty() && suborders.isEmpty()) {
			// Optimization: If all of the specified filters are not restricted, 
			// establishes the offset and the limit at the query execution.
			options = FetchOptions.Builder.withOffset(offset);
			
			if (Integer.MAX_VALUE - limit >= offset) {
				options.limit(limit);
			}
		}
		
		int count = 0;
		List<E> result = new ArrayList<E>();
		
		for (Entity entity : session.datastore().query(query.setKeysOnly(), options)) {
			E e = session.get(type, entity.getKey());
			Iterator<Filter<?>> iterator = subfilters.iterator();
			boolean matches = true;
			
			while (iterator.hasNext()) {
				if (!iterator.next().matches(e)) {
					matches = false;
					break;
				}
			}
			
			if (matches) {
				result.add(e);
				count++;
				
				if (count >= limit) {
					break;
				}
			}
		}
		
		if (!suborders.isEmpty()) {
			Collections.sort(result, new Comparator<E>() {

				@SuppressWarnings("unchecked")
				@Override
				public int compare(E entity1, E entity2) {
					int compare = 0;
					
					for (Order<?, ?> o : suborders) {
						Order<E, ?> order = (Order<E, ?>) o;
						compare = order.compare(entity1, entity2);
						
						if (order.direction() == Order.Direction.DESC) {
							compare = compare * -1;
						}
						
						if (compare != 0) {
							return compare;
						}
					}
					
					return compare;
				};
				
			});
		}
		
		if (subfilters.isEmpty()) {
			return result;
		} else {
			int offset = this.offset;
			
			if (offset > result.size()) {
				offset = result.size() - 1;
			}
			
			int limit = offset + this.limit;
			
			if (limit > result.size()) {
				limit = result.size();
			}
			
			return result.subList(offset, limit);
		}
	}

	/**
	 * Specifies sort orders by which the returned list is sorted.
	 * 
	 * @param orders The sort orders which the returned list is sorted by.
	 * @return The {@code GetList} which the execution result is sorted 
	 * by the specified sort orders.
	 */
	@Override
	public AppEngineGetList<E> sort(Order<?, ?>... orders) {
		if (orders == null) {
			throw new IllegalArgumentException("'orders' must not be [" + orders + "]");
		}
		
		this.orders = Arrays.asList(orders);
		return this;
	}

	/**
	 * Qualifies entities to be retrieved with the specified {@code Filter}s.
	 * Each of the specified filters is combined with "logical and".
	 * 
	 * @param filters {@code Filter}s to qualify entities to be retrieved.
	 * @return The {@code GetList} which the execution result is 
	 * qualified with the specified {@code Filter}s.
	 */
	@Override
	public AppEngineGetList<E> filter(Filter<?>... filters) {
		if (filters == null) {
			throw new IllegalArgumentException("'filters' must not be [" + null + "]");
		}
		
		this.filters = Arrays.asList(filters);
		return this;
	}

	/**
	 * Qualifies limit size of the returned list.
	 * 
	 * @param limit The limit size of the returned list.
	 * @return The {@code GetList} which the execution result is qualified with 
	 * the specified limit.
	 */
	@Override
	public AppEngineGetList<E> limit(int limit) {
		if (limit < 1) {
			throw new IllegalArgumentException("'limit' must be greater than or equal to [" + 1 + "]");
		}
		
		this.limit = limit;
		return this;
	}

	/**
	 * Qualifies offset of the execution result to be contained in the returned 
	 * list at first.
	 * 
	 * @param offset The offset of the execution result to be contained in the 
	 * returned list at first.
	 * @return The {@code GetList} which the execution result is qualified with 
	 * the specified offset.
	 */
	@Override
	public AppEngineGetList<E> offset(int offset) {
		if (offset < 0) {
			throw new IllegalArgumentException("'offset' must be greater than or equal to [" + 0 + "]");
		}
		
		this.offset = offset;
		return this;
	}

}
