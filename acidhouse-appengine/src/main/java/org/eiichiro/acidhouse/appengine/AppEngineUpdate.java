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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.GetList;
import org.eiichiro.acidhouse.Update;
import org.eiichiro.acidhouse.metamodel.Metamodel;
import org.eiichiro.acidhouse.metamodel.Property;

/**
 * {@code AppEngineUpdate} is a App Engine Low-level Datastore API based 
 * implentation of {@code Update}.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineUpdate<E> implements Update<E> {

	private Logger logger = Logger.getLogger(getClass().getName());
	
	private final Metamodel<E> metamodel;
	
	private final AppEngineDatastoreSession session;
	
	private List<Filter<?>> filters = new ArrayList<Filter<?>>(0);
	
	private Map<Property<?, ?>, Object> properties = new HashMap<Property<?, ?>, Object>(0);
	
	/**
	 * Constructs a new {@code AppEngineUpdate} with the specified metamodel of 
	 * entity and {@code AppEngineDatastoreSession}.
	 * 
	 * @param metamodel The metamodel of entity to update with this command.
	 * @param session {@code AppEngineDatastoreSession} instance for this command.
	 */
	public AppEngineUpdate(Metamodel<E> metamodel, AppEngineDatastoreSession session) {
		this.metamodel = metamodel;
		this.session = session;
	}
	
	/**
	 * Executes {@code AppEngineUpdate} with {@code AppEngineDatastoreSession}.
	 * 
	 * @return The number of updated entity.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Integer execute() {
		logger.fine("Executing [AppEngineUpdate] command");
		
		if (properties.size() == 0) {
			throw new IllegalStateException("'properties' must be set");
		}
		
		GetList<E> command = session.get(metamodel);
		
		if (filters != null) {
			command.filter(filters.toArray(new Filter<?>[] {}));
		}
		
		int i = 0;
		
		for (E entity : command.execute()) {
			for (Property<?, ?> property : properties.keySet()) {
				Object object = properties.get(property);
				
				if (object instanceof Modification) {
					Modification<Object> modification = (Modification<Object>) object;
					((Property<E, ?>) property).set(entity, modification.apply(property.get(entity)));
				} else {
					((Property<E, ?>) property).set(entity, object);
				}
			}
			
			session.update(entity);
			i++;
		}
		
		return i;
	}

	/**
	 * Qualifies property and the value to be updated.
	 * 
	 * @param property The property to be updated by this 
	 * {@code Update}.
	 * @param value The property value to be updated to.
	 * @return The {@code Update} to update the specified entity 
	 * properties.
	 */
	@Override
	public <T> AppEngineUpdate<E> set(Property<?, T> property, T value) {
		if (property == null) {
			throw new IllegalArgumentException("'property' must not be [" + property + "]");
		}
		
		properties.put(property, value); 
		return this;
	}

	/**
	 * Qualifies property to be updated and the modification function.
	 * 
	 * @param property The property to be updated by this 
	 * {@code Update}.
	 * @param modification The modification function applied to the property.
	 * @return The {@code Update} to update the specified entity 
	 * properties.
	 */
	@Override
	public <T> Update<E> set(Property<?, T> property, Modification<T> modification) {
		if (property == null) {
			throw new IllegalArgumentException("'property' must not be [" + property + "]");
		}
		
		properties.put(property, modification); 
		return this;
	}
	
	/**
	 * Qualifies entities to be updated with the specified {@code Filter}s.
	 * Each of the specified filters is combined with "logical and".
	 * 
	 * @param filters {@code Filter}s to qualify entities to be updated.
	 * @return The {@code Update} to update entities qualified with the 
	 * specified {@code Filter}s.
	 */
	@Override
	public AppEngineUpdate<E> filter(Filter<?>... filters) {
		if (filters == null) {
			throw new IllegalArgumentException("'filters' must not be [" + filters + "]");
		}
		
		this.filters = Arrays.asList(filters);
		return this;
	}

}
