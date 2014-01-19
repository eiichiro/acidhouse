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

import static org.eiichiro.acidhouse.appengine.Version.*;

import java.util.List;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.DatastoreAttributes.DatastoreType;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.api.utils.SystemProperty;

/**
 * {@code AppEngineDatastoreService} is a convenient wrapper of 
 * {@code com.google.appengine.api.datastore.DatastoreService}.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineDatastoreService {

	private Logger logger = Logger.getLogger(getClass().getName());
	
	static {
		Logger logger = Logger.getLogger(Version.class.getName());
		logger.info("Acid House App Engine " + MAJOR + "." + MINER + "." + BUILD);
//		logger.info("Copyright (C) 2009-2011 Eiichiro Uchiumi. All Rights Reserved.");
		logger.info("Application [" + SystemProperty.applicationId.get()
				+ "] is running in [" + SystemProperty.environment.value()
				+ "] environment");
	}
	
	private final DatastoreService datastore;
	
	private DatastoreType datastoreType;
	
	/**
	 * Constructs a new {@code AppEngineDatastoreService} with the specified 
	 * {@code com.google.appengine.api.datastore.DatastoreService}.
	 * 
	 * @param datastore com.google.appengine.api.datastore.DatastoreService.
	 */
	public AppEngineDatastoreService(DatastoreService datastore) {
		this.datastore = datastore;
		datastoreType = datastore.getDatastoreAttributes().getDatastoreType();
		logger.fine("Application [" + SystemProperty.applicationId.get()
				+ "] is using [" + datastoreType + "] datastore");
	}
	
	/**
	 * Gets App Engine Datastore entity corresponding to the specified 
	 * {@code Key}. If the entity doesn't exist in the datastore, this method 
	 * returns {@code null}.
	 * 
	 * @param key The key to get the entity.
	 * @return The entity corresponding to the specified {@code Key}.
	 */
	public Entity get(Key key) {
		try {
			return datastore.get(key);
		} catch (EntityNotFoundException e) {
			return null;
		}
	}
	
	/**
	 * Gets App Engine Datastore entity corresponding to the specified 
	 * {@code Key} within the specified {@code Transaction}.
	 * 
	 * @param transaction The transaction to get the entity.
	 * @param key The key to get the entity.
	 * @return The entity corresponding to the specified {@code Key}.
	 */
	public Entity get(Transaction transaction, Key key) {
		try {
			return datastore.get(transaction, key);
		} catch (EntityNotFoundException e) {
			return null;
		}
	}

	/**
	 * Puts the specified App Engine Datastore entity into the datastore within 
	 * the specified {@code Transaction}.
	 * 
	 * @param transaction The transaction to put the entity.
	 * @param entity The entity to be put.
	 * @return The {@code Key} allocated to the stored entity.
	 */
	public Key put(Transaction transaction, Entity entity) {
		return datastore.put(transaction, entity);
	}
	
	/**
	 * Puts the specified App Engine Datastore entities into the datastore 
	 * within the specified {@code Transaction}.
	 * 
	 * @param transaction The transaction to put the entities.
	 * @param entities The entities to be put.
	 * @return The keys allocated to the stored entities.
	 */
	public List<Key> put(Transaction transaction, Iterable<Entity> entities) {
		return datastore.put(transaction, entities);
	}
	
	/**
	 * Deletes entities corresponding to the specified {@code Key}s from App 
	 * Engine Datastore within the specified {@code Transaction}.
	 * 
	 * @param transaction The transaction to delete entities.
	 * @param keys The {@code Key}s corresponding to the entities to be deleted.
	 */
	public void delete(Transaction transaction, Iterable<Key> keys) {
		datastore.delete(transaction, keys);
	}
	
	/**
	 * Deletes entities corresponding to the specified {@code Key}s from App 
	 * Engine Datastore within the specified {@code Transaction}.
	 * 
	 * @param transaction The transaction to delete entities.
	 * @param keys The {@code Key}s corresponding to the entities to be deleted.
	 */
	public void delete(Transaction transaction, Key... keys) {
		datastore.delete(transaction, keys);
	}
	
	/**
	 * Begins transaction and returns it.
	 * If the current application is using High Replication Datastore, this 
	 * method returns XG (Cross-Group) transaction.
	 * 
	 * @return Current transaction.
	 */
	public Transaction beginTransaction() {
		if (datastoreType == DatastoreType.HIGH_REPLICATION) {
			return datastore.beginTransaction(TransactionOptions.Builder.withXG(true));
		}
		
		return datastore.beginTransaction();
	}
	
	/**
	 * Executes the specified query without an offset qualification.
	 * 
	 * @param paramQuery The query to be executed.
	 * @return The query result as {@code List} view.
	 */
	public List<Entity> query(Query query) {
		return query(query, FetchOptions.Builder.withOffset(0));
	}
	
	/**
	 * Executes the specified query within the specified transaction without an 
	 * offset qualification.
	 * 
	 * @param transaction The transaction in which the specified query is 
	 * executed.
	 * @param query The query to be executed.
	 * @return The query result as {@code List} view.
	 */
	public List<Entity> query(Transaction transaction, Query query) {
		return query(transaction, query, FetchOptions.Builder.withOffset(0));
	}
	
	/**
	 * Executes the specified query with the specified fetch options.
	 * 
	 * @param query The query to be executed.
	 * @param options The fetch options which the specified query is executed 
	 * with.
	 * @return The query result as {@code List} view.
	 */
	public List<Entity> query(Query query, FetchOptions options) {
		return datastore.prepare(query).asList(options);
	}
	
	/**
	 * Executes the specified query in the specified transaction with the 
	 * specified fetch options.
	 * 
	 * @param transaction The transaction in which the specified query is 
	 * executed.
	 * @param query The query to be executed.
	 * @param options The fetch options which the specified query is executed 
	 * with.
	 * @return The query result as {@code List} view.
	 */
	public List<Entity> query(Transaction transaction, Query query, FetchOptions options) {
		return datastore.prepare(transaction, query).asList(options);
	}
	
	/**
	 * Returns the deadline of datastore operation as millisecond.
	 * 
	 * @return The deadline of datastore operation as millisecond.
	 */
	public double deadline() {
		return 60000;
	}

	void datastoreType(DatastoreType datastoreType) {
		this.datastoreType = datastoreType;
	}
	
}
