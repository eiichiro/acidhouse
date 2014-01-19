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

import static org.eiichiro.acidhouse.Entities.*;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.Aggregation;
import org.eiichiro.acidhouse.Session;
import org.eiichiro.acidhouse.EntityExistsException;
import org.eiichiro.acidhouse.GetList;
import org.eiichiro.acidhouse.Log;
import org.eiichiro.acidhouse.Log.State;
import org.eiichiro.acidhouse.metamodel.Metamodel;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

/**
 * {@code AppEngineDatastoreSession} is a App Engine Low-level API based 
 * {@code Session} implementation. This implementation supports 
 * loose consistency in multiple entity groups transaction commitment. It 
 * performs faster than {@link AppEngineStrongDatastoreSession} (Strong session), 
 * however, the transaction completion protocol in this session may be in-doubt 
 * condition if the transaction commitment is failed partially due to 
 * transaction timeout or unexpected exception. When the consistency is more 
 * important in your requirement rather than the throughput, you should use 
 * Strong session. Also, you must get entities committed by Strong session 
 * with Strong session.
 * 
 * @see AppEngineStrongDatastoreSession
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineDatastoreSession implements Session {

	private final Logger logger = Logger.getLogger(getClass().getName());
	
	/** {@code AppEngineDatastoreService}. */
	private final AppEngineDatastoreService datastore;
	
	/** The transaction this {@code Session} has begun. */
	protected ThreadLocal<AppEngineTransaction> transaction = new ThreadLocal<AppEngineTransaction>();
	
	/** Constructs a new {@code AppEngineDatastoreSession}. */
	public AppEngineDatastoreSession() {
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		this.datastore = new AppEngineDatastoreService(datastore);
	}
	
	/**
	 * Begins transaction. You have to do transactional operation after calling 
	 * this method.
	 * 
	 * @return The transaction begun.
	 */
	@Override
	public AppEngineTransaction beginTransaction() {
		if (isTransactional()) {
			throw new IllegalStateException("Transaction has already started ["
					+ transaction.get().id() + "]");
		}
		
		AppEngineTransaction tx = new AppEngineTransaction(this);
		transaction.set(tx);
		logger.fine("Transaction [" + transaction.get().id() + "] started");
		return tx;
	}

	/** Closes this session. */
	@Override
	public void close() {
		AppEngineTransaction tx = transaction.get();
		
		if (tx != null) {
			tx.rollback();
		}
	}

	/**
	 * Returns the entity instance corresponding to the specified key. This 
	 * method returns {@code null} if the entity is not found.
	 * 
	 * @param <E> The entity type.
	 * @param clazz The entity type that you want to get.
	 * @param key The key corresponding to the entity that you want to get.
	 * @return The entity instance corresponding to the specified key.
	 * @throws ConcurrentModificationException If the entity corresponding to 
	 * the specified key is being modified by the other transaction.
	 * @see AppEngineResourceManager#get(Class, Object)
	 */
	@Override
	public <E> E get(Class<E> clazz, Object key)
			throws ConcurrentModificationException {
		if (clazz == null) {
			throw new IllegalArgumentException("'clazz' must not be [" + clazz + "]");
		}
		
		if (key == null) {
			throw new IllegalArgumentException("'id' must not be [" + key + "]");
		}
		
		Key k = Keys.create(Translation.toKind(clazz), key);
		Map<Key, Object> references = new HashMap<Key, Object>();
		
		if (!isTransactional()) {
			return Translation.toObject(
					clazz, datastore.query(new Query(k)), references, datastore);
		} else {
			AppEngineTransaction tx = transaction.get();
			E entity = Translation.toObject(
					clazz, datastore.query(tx.transaction(), new Query(k)), references, datastore);
			
			if (entity == null) {
				return entity;
			}
			
			List<Log> logs = tx.logs();
			logs.add(new Log(logs.size() + 1, Log.Operation.GET, entity));
			return entity;
		}
	}

	/**
	 * Puts the specified entity instance into App Engine Datastore newly.
	 * This method must be invoked under a transaction. This method throws 
	 * {@code EntityExistsException} if the entity that has the same key as 
	 * the specified entity has been already stored.
	 * 
	 * @param entity The entity instance to be put into App Engine Datastore.
	 * @throws EntityExistsException If the entity that has the same key as the 
	 * specified entity has already existed.
	 */
	@Override
	public void put(Object entity) throws EntityExistsException {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' must not be [" + entity + "]");
		}
		
		assertTransactional();
		
		for (Log log : transaction.get().logs()) {
			if (keyValue(entity).equals(keyValue(log.entity()))) {
				throw new IllegalStateException("Log [" + log.operation()
						+ "] -> [" + Log.Operation.PUT
						+ "] is not allowed: This operation must be first");
			}
		}
		
		Transaction tx = transaction.get().transaction();
		Key key = Keys.create(Translation.toKind(entity.getClass()), keyValue(entity));
		Entity e = datastore.get(tx, key);
		
		if (e != null) {
			throw new EntityExistsException(key);
		}
		
		datastore.put(tx, Translation.toEntities(entity));
		List<Log> logs = transaction.get().logs();
		Log log = new Log(logs.size() + 1, Log.Operation.PUT, entity);
		log.state(State.UNCOMMITTED);
		logs.add(log);
	}

	/**
	 * Updates the entity in the datastore with the specified one. 
	 * This method must be invoked under a transaction.
	 * 
	 * @param entity The entity instance to be updated to.
	 */
	@Override
	public void update(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' must not be [" + entity + "]");
		}
		
		assertTransactional();
		List<Log> logs = transaction.get().logs();
		
		if (logs.isEmpty()) {
			throw new IllegalStateException("Log ["
					+ Log.Operation.UPDATE
					+ "] is not allowed: previous operation must be ["
					+ Log.Operation.GET + "]");
		}
		
		for (int i = logs.size() - 1; i >= 0; i--) {
			Log log = logs.get(i);
			
			if (keyValue(entity).equals(keyValue(log.entity()))) {
				if (log.operation() == Log.Operation.GET) {
					break;
				} else {
					throw new IllegalStateException("Log [" + log.operation() 
							+ "] -> [" + Log.Operation.UPDATE
							+ "] is not allowed: previous operation must be ["
							+ Log.Operation.GET + "]");
				}
			}
		}
		
		Transaction tx = transaction.get().transaction();
		datastore.put(tx, Translation.toEntities(entity));
		Log log = new Log(logs.size() + 1, Log.Operation.UPDATE, entity);
		log.state(State.UNCOMMITTED);
		logs.add(log);
	}

	/** Ensures the transaction has been started. */
	protected void assertTransactional() {
		if (!isTransactional()) {
			throw new IllegalStateException("Transaction has not started: "
					+ "Call [Session#beginTransaction()] "
					+ "before doing this");
		}
	}
	
	/**
	 * Deletes the specified entity from the datastore. 
	 * This method must be invoked under a transaction.
	 * 
	 * @param entity The entity to be deleted.
	 */
	@Override
	public void delete(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' must not be [" + entity + "]");
		}
		
		assertTransactional();
		List<Log> logs = transaction.get().logs();
		
		if (logs.isEmpty()) {
			throw new IllegalStateException("Log ["
					+ Log.Operation.DELETE
					+ "] is not allowed: previous operation must be ["
					+ Log.Operation.GET + "]");
		}
		
		for (int i = logs.size() - 1; i >= 0; i--) {
			Log log = logs.get(i);
			
			if (keyValue(entity).equals(keyValue(log.entity()))) {
				if (log.operation() == Log.Operation.GET) {
					break;
				} else {
					throw new IllegalStateException("Log [" + log.operation() 
							+ "] -> [" + Log.Operation.DELETE
							+ "] is not allowed: previous operation must be ["
							+ Log.Operation.GET + "]");
				}
			}
		}
		
		Transaction tx = transaction.get().transaction();
		List<Key> keys = new ArrayList<Key>();
		
		for (Entity e : Translation.toEntities(entity)) {
			keys.add(e.getKey());
		}
		
		datastore.delete(tx, keys);
		Log log = new Log(logs.size() + 1, Log.Operation.DELETE, entity);
		log.state(State.UNCOMMITTED);
		logs.add(log);
	}

	/**
	 * Returns {@code GetList} command based on App Engine Low-level Datastore 
	 * API for the specified entity metamodel.
	 * This method is the entry point for getting-list Command Builder API.
	 * 
	 * @param <E> The entity type to get with this {@code GetList}.
	 * @param metamodel The metamodel of the entity type to get with this 
	 * {@code GetList}. 
	 * @return {@code GetList} for the specified entity metamodel.
	 * @see GetList
	 */
	@Override
	public <E> AppEngineGetList<E> get(Metamodel<E> metamodel) {
		if (metamodel == null) {
			throw new IllegalArgumentException("'metamodel' must not be [" + metamodel + "]");
		}
		
		return new AppEngineGetList<E>(metamodel, this);
	}

	/**
	 * Returns {@code GetScalar} command based on App Engine Low-level Datastore 
	 * API for the specified {@code Aggregation}.
	 * This method is the entry point for aggregation Command Builder API.
	 * 
	 * @param <E> The entity type.
	 * @param <R> The property value type to aggregate with this 
	 * {@code GetScalar}.
	 * @param aggregation The {@code Aggregation} this method executes.
	 * @return {@code GetScalar} for the specified {@code Aggregation} instance.
	 */
	@Override
	public <E, R> AppEngineGetScalar<E, R> get(Aggregation<R> aggregation) {
		if (aggregation == null) {
			throw new IllegalArgumentException("'aggregation' must not be [" + aggregation + "]");
		}
		
		return new AppEngineGetScalar<E, R>(aggregation, this);
	}

	/**
	 * Returns {@code Update} command based on App Engine Low-level Datastore 
	 * API for the specified entity metamodel.
	 * This method is the entry point for updating Command Builder API.
	 * 
	 * @param <E> The type of entity updated by this {@code Update}.
	 * @param metamodel The metamodel of the entity updated by this {@code Update}.
	 * @return The {@code Update} for the specified entity class.
	 */
	@Override
	public <E> AppEngineUpdate<E> update(Metamodel<E> metamodel) {
		if (metamodel == null) {
			throw new IllegalArgumentException("'metamodel' must not be [" + metamodel + "]");
		}
		
		return new AppEngineUpdate<E>(metamodel, this);
	}

	/**
	 * Returns {@code Delete} command based on App Engine Low-level Datastore 
	 * API for the specified entity metamodel.
	 * This method is the entry point for deleting Command Builder API.
	 * 
	 * @param <E> The type of entity deleted by this {@code Delete}.
	 * @param metamodel The metamodel of the entity deleted by this 
	 * {@code Delete}.
	 * @return The {@code Delete} for the specified entity metamodel.
	 */
	@Override
	public <E> AppEngineDelete<E> delete(Metamodel<E> metamodel) {
		if (metamodel == null) {
			throw new IllegalArgumentException("'metamodel' must not be [" + metamodel + "]");
		}
		
		return new AppEngineDelete<E>(metamodel, this);
	}
	
	/**
	 * Returns the transaction has been begun or not.
	 * 
	 * @return The transaction has been begun or not.
	 */
	public boolean isTransactional() {
		return (transaction.get() != null);
	}

	AppEngineDatastoreService datastore() {
		return datastore;
	}
	
}
