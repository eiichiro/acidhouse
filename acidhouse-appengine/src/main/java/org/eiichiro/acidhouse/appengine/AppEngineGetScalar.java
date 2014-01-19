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
package org.eiichiro.acidhouse.appengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.Aggregation;
import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.GetList;
import org.eiichiro.acidhouse.GetScalar;
import org.eiichiro.acidhouse.Order;
import org.eiichiro.acidhouse.metamodel.Metamodel;
import org.eiichiro.acidhouse.metamodel.Property;

/**
 * {@code AppEngineGetScalar} is a App Engine Low-level Datastore API based 
 * implementation of {@code GetScalar}.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineGetScalar<E, R> implements GetScalar<E, R> {

	private Logger logger = Logger.getLogger(getClass().getName());
	
	private final Aggregation<R> aggregation;
	
	private final AppEngineDatastoreSession session;
	
	private List<Filter<?>> filters = new ArrayList<Filter<?>>(0);
	
	private Order<?, ?>[] orders;
	
	/**
	 * Constructs a new {@code AppEngineGetScalar} with the specified 
	 * {@code Aggregation} and {@code AppEngineDatastoreSession}.
	 * 
	 * @param aggregation {@code Aggregation} for this command.
	 * @param session {@code AppEngineDatastoreSession} instance for this command.
	 */
	public AppEngineGetScalar(Aggregation<R> aggregation, AppEngineDatastoreSession session) {
		this.aggregation = aggregation;
		this.session = session;
	}
	
	/**
	 * Executes {@code GetScalar} with {@code AppEngineDatastoreSession}.
	 * 
	 * @return The result of the specified {@code Aggregation}.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public R execute() {
		logger.fine("Executing [AppEngineGetScalar] command");
		Property<?, R> property = aggregation.property();
		GetList<E> command = session.get((Metamodel<E>) property.metamodel().root());
		
		if (filters != null) {
			command.filter(filters.toArray(new Filter<?>[] {}));
		}
		
		if (orders != null) {
			command.sort(orders);
		}
		
		List<E> entities = command.execute();
		List<R> list = new ArrayList<R>();
		
		for (Object entity : entities) {
			list.add(property.get(entity));
		}
		
		return aggregation.aggregate(list);
	}

	/**
	 * Specifies sort orders which the list passed to 
	 * {@code Aggregation#aggregate(java.util.List)} is sorted by.
	 * 
	 * @param orders The sort orders which the list passed to 
	 * {@code Aggregation#aggregate(java.util.List)} is sorted by.
	 * @return The {@code GetScalar} which the sort orders have been specified.
	 */
	@Override
	public AppEngineGetScalar<E, R> sort(Order<?, ?>... orders) {
		if (orders == null) {
			throw new IllegalArgumentException("'orders' must not be [" + orders + "]");
		}
		
		this.orders = orders;
		return this;
	}

	/**
	 * Qualifies entities to be retrieved with the specified {@code Filter}s.
	 * Each of the specified filters is combined with "logical and".
	 * 
	 * @param filters {@code Filter}s to qualify the retrieved entities.
	 * @return The {@code GetScalar} which the execution result passed to 
	 * {@code Aggregation#aggregate(java.util.List)} is qualified by the 
	 * specified filters.
	 */
	@Override
	public AppEngineGetScalar<E, R> filter(Filter<?>... filters) {
		if (filters == null) {
			throw new IllegalArgumentException("'filters' must not be [" + filters + "]");
		}
		
		this.filters = Arrays.asList(filters);
		return this;
	}

}
