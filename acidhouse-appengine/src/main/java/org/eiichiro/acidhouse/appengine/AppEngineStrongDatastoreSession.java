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

import java.util.ConcurrentModificationException;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.Session;
import org.eiichiro.acidhouse.EntityExistsException;
import org.eiichiro.acidhouse.ResourceManager;

import com.google.appengine.api.datastore.Transaction;

/**
 * {@code AppEngineStrongDatastoreSession} is a App Engine Low-level Datastore 
 * API based {@code Session} implementation called "Strong session". 
 * This implementation supports the strong consistency in multiple entity groups 
 * manipulation by the atomic commitment and consistent read.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineStrongDatastoreSession extends AppEngineDatastoreSession
		implements Session {

	private final Logger logger = Logger.getLogger(getClass().getName());
	
	/**
	 * Begins {@code Transaction} with {@code AppEngineCoordinator}.
	 * You have to do transactional operation after calling this method.
	 * 
	 * @return The transaction begun.
	 */
	@Override
	public AppEngineTransaction beginTransaction() {
		if (isTransactional()) {
			throw new IllegalStateException("Transaction has already started ["
					+ transaction.get().id() + "]");
		}
		
		transaction.set(new AppEngineGlobalTransaction(this));
		logger.fine("Transaction ["
				+ transaction.get().id()
				+ "] started");
		return transaction.get();
	}

	/**
	 * Returns the entity instance corresponding to the specified key. 
	 * This method returns {@code null} if the entity is not found.
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
	public <E> E get(Class<E> clazz, Object key) throws ConcurrentModificationException {
		if (clazz == null) {
			throw new IllegalArgumentException("'clazz' must not be [" + clazz + "]");
		}
		
		if (key == null) {
			throw new IllegalArgumentException("'id' must not be [" + key + "]");
		}
		
		if (!isTransactional()) {
			ResourceManager<Transaction> manager = new AppEngineResourceManager(datastore());
			return manager.get(clazz, key);
		} else {
			return ((AppEngineGlobalTransaction) transaction.get()).coordinator().get(clazz, key);
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
		((AppEngineGlobalTransaction) transaction.get()).coordinator().put(entity);
	}
	
	/**
	 * Updates entity with the specified entity instance with 
	 * {@code AppEngineCoordinator}.
	 * This method must be invoked under a transaction. 
	 * 
	 * @param entity The entity instance to be updated.
	 */
	@Override
	public void update(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' must not be [" + entity + "]");
		}
		
		assertTransactional();
		((AppEngineGlobalTransaction) transaction.get()).coordinator().update(entity);
	}
	
	/**
	 * Deletes the specified entity from App Engine Datastore with 
	 * {@code AppEngineCoordinator}.
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
		((AppEngineGlobalTransaction) transaction.get()).coordinator().delete(entity);
	}
	
}
