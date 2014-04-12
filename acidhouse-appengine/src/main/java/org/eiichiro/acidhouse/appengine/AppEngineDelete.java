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

import org.eiichiro.acidhouse.Delete;
import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.GetList;
import org.eiichiro.acidhouse.metamodel.Metamodel;

/**
 * {@code AppEngineDelete} is a App Engine Low-level Datastore API based 
 * implementation of {@code Delete}.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineDelete<E> implements Delete<E> {

	private Logger logger = Logger.getLogger(getClass().getName());
	
	private final Metamodel<E> metamodel;

	private final AppEngineDatastoreSession session;
	
	private List<Filter<?>> filters = new ArrayList<Filter<?>>(0);
	
	/**
	 * Constructs a new {@code AppEngineDelete} with the specified metamodel of 
	 * entity and {@code AppEngineDatastoreSession}.
	 * 
	 * @param metamodel The metamodel of entity to delete with this command.
	 * @param session {@code AppEngineDatastoreSession} instance for this command.
	 */
	public AppEngineDelete(Metamodel<E> metamodel, AppEngineDatastoreSession session) {
		this.metamodel = metamodel;
		this.session = session;
	}
	
	/**
	 * Executes {@code AppEngineDelete} with {@code AppEngineDatastoreSession}.
	 * 
	 * @return The number of deleted entity.
	 */
	@Override
	public Integer execute() {
		logger.fine("Executing [AppEngineDelete] command");
		GetList<E> command = session.get(metamodel);
		
		if (filters != null) {
			command.filter(filters.toArray(new Filter<?>[] {}));
		}
		
		int i = 0;
		
		for (E entity : command.execute()) {
			session.delete(entity);
			i++;
		}
		
		return i;
	}

	/**
	 * Qualifies entities to be deleted with the specified {@code Filter}s.
	 * Each of the specified filters is combined with "logical and".
	 * 
	 * @param filters {@code Filter}s to qualify entities to be deleted.
	 * @return The {@code Delete} to delete entities qualified with the 
	 * specified {@code Filter}s.
	 */
	@Override
	public AppEngineDelete<E> filter(Filter<?>... filters) {
		if (filters == null) {
			throw new IllegalArgumentException("'filters' must not be [" + filters + "]");
		}
		
		this.filters = Arrays.asList(filters);
		return this;
	}

}
