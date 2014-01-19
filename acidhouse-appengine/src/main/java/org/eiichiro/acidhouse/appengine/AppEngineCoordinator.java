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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.Coordinator;
import org.eiichiro.acidhouse.IndoubtException;
import org.eiichiro.acidhouse.Log;
import org.eiichiro.acidhouse.Log.State;
import org.eiichiro.acidhouse.ResourceManager;
import org.eiichiro.reverb.lang.UncheckedException;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;

/**
 * {@code AppEngineCoordinator} is a App Engine Low-level Datastore API based 
 * {@code Coordinator} implementation.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineCoordinator implements Coordinator {

	private Logger logger = Logger.getLogger(getClass().getName());
	
	private final AppEngineGlobalTransaction transaction;
	
	private final AppEngineDatastoreService datastore;
	
	private final Map<Object, ResourceManager<Transaction>> managers 
			= new HashMap<Object, ResourceManager<Transaction>>();
	
	/**
	 * Constructs a new {@code AppEngineCoordinator} instance with the 
	 * {@code AppEngineGlobalTransaction} and 
	 * {@code AppEngineDatastoreService}.
	 * 
	 * @param transaction {@code GlobalTransaction}.
	 * @param datastore {@code AppEngineDatastoreService}.
	 */
	public AppEngineCoordinator(AppEngineGlobalTransaction transaction, 
			AppEngineDatastoreService datastore) {
		this.transaction = transaction;
		this.datastore = datastore;
	}

	/**
	 * Gets the entity instance of the specified {@code Class} corresponding to 
	 * the specified key from the datastore within the current transaction.
	 * 
	 * @param <E> The type of entity.
	 * @param clazz The {@code Class} of entity.
	 * @param key The key of the entity you attempt to get.
	 * @return The entity instance of the specified {@code Class} corresponding 
	 * to the specified key.
	 */
	@Override
	public <E> E get(Class<E> clazz, Object key) {
		ResourceManager<Transaction> manager = new AppEngineResourceManager(
				datastore, datastore.beginTransaction(), transaction);
		E entity = manager.get(clazz, key);
		managers.put(key, manager);
		transaction.logs().add(new Log(transaction.logs().size() + 1,
				Log.Operation.GET, entity));
		logger.fine("Transaction [" + transaction.id() + "]: Entity [" + entity + "] has been get");
		return entity;
	}

	/**
	 * Puts the specified entity instance into the datastore newly within the 
	 * current transaction.
	 * 
	 * @param entity The entity to be put into the datastore.
	 */
	@Override
	public void put(Object entity) {
		ResourceManager<Transaction> manager = new AppEngineResourceManager(
				datastore, datastore.beginTransaction(), transaction);
		manager.put(entity);
		Log log = new Log(transaction.logs().size() + 1, Log.Operation.PUT, entity);
		log.state(State.UNCOMMITTED);
		transaction.logs().add(log);
		managers.put(keyValue(entity), manager);
		logger.fine("Transaction [" + transaction.id() + "]: Entity [" + entity + "] has been put");
	}

	/**
	 * Updates the entity in the datastore with the specified one within the 
	 * current transaction.
	 * 
	 * @param entity The entity to be updated to.
	 */
	@Override
	public void update(Object entity) {
		ResourceManager<Transaction> manager = managers.get(keyValue(entity));
		
		if (manager == null) {
			throw new IllegalStateException("Log ["
					+ Log.Operation.UPDATE
					+ "] is not allowed: previous operation must be ["
					+ Log.Operation.GET + "]");
		}
		
		manager.update(entity);
		Log log = new Log(transaction.logs().size() + 1, Log.Operation.UPDATE, entity);
		log.state(State.UNCOMMITTED);
		transaction.logs().add(log);
		logger.fine("Transaction [" + transaction.id() + "]: Entity [" + entity + "] has been updated");
	}

	/**
	 * Deletes the specified entity instance from the datastore within the 
	 * current transaction.
	 * 
	 * @param entity The entity to be deleted.
	 */
	@Override
	public void delete(Object entity) {
		ResourceManager<Transaction> manager = managers.get(keyValue(entity));
		
		if (manager == null) {
			throw new IllegalStateException("Log ["
					+ Log.Operation.DELETE
					+ "] is not allowed: previous operation must be ["
					+ Log.Operation.GET + "]");
		}
		
		manager.delete(entity);
		Log log = new Log(transaction.logs().size() + 1, Log.Operation.DELETE, entity);
		log.state(State.UNCOMMITTED);
		transaction.logs().add(log);
		logger.fine("Transaction [" + transaction.id() + "]: Entity [" + entity + "] has been deleted");
	}

	/**
	 * Commits every operation in the current transaction with "Two-phase commit 
	 * protocol".
	 * Optimization: If the current transaction has just one operation, this 
	 * method commits it by Single-phase commit protocol.<br>
	 * In the preparation phase, this method invokes every {@code ResourceManager}'s 
	 * {@code ResourceManager#prepare()} method to lock the managing entity. If 
	 * this phase failed due to any exceptions, {@code AppEngineCoordinator} 
	 * considers the current transaction commitment is aborted. If the 
	 * preparation phase completed successfully, this method determines the 
	 * current transaction commitment is succeeded and then starts the 
	 * commitment (apply) phase.
	 * In the commitment phase, this method invokes every {@code ResourceManager}'s 
	 * {@code ResourceManager#commit()} method to apply the transactional 
	 * operation to the entity and to free the allocated lock. If 
	 * {@code ResourceManager#commit()} method fails due to any exceptions, this 
	 * method logs the failure and returns immediately. Uncommitted operation is 
	 * applied when the entity is read after this with {@code ResourceManager} 
	 * (Consistent read).
	 * 
	 * @see AppEngineResourceManager
	 */
	@Override
	public void commit() throws IndoubtException {
		List<Log> logs = transaction.logs();
		int group = 0;
		int last = 0;
		
		for (int i = 0; i < logs.size(); i++) {
			if (logs.get(i).operation() != Log.Operation.GET) {
				group++;
				last = i;
			}
		}
		
		if (group == 0) {
			return;
		}
		
		// Optimization: Processes by Single-phase commit (local transaction) if 
		// this transaction has just one entity group.
		if (group == 1) {
			logger.fine("Transaction [" + transaction.id() + "]: Completion started (Single-phase commitment)");
			Log log = logs.get(last);
			Object entity = log.entity();
			ResourceManager<Transaction> manager = managers.get(keyValue(entity));
			List<Entity> entities = Translation.toEntities(entity);
			
			if (log.operation() != Log.Operation.DELETE) {
				datastore.put(manager.transaction(), entities);
			} else {
				List<Key> deletes = new ArrayList<Key>();
				
				for (Entity e : entities) {
					deletes.add(e.getKey());
				}
				
				datastore.delete(manager.transaction(), deletes);
			}
			
			manager.transaction().commit();
			log.state(State.COMMITTED);
			logger.fine("Transaction [" + transaction.id() + "]: Transaction has been committed");
			return;
		}
		
		// Two-phase commit protocol. Phase 1: Preparation phase.
		logger.fine("Transaction [" + transaction.id() + "]: Completion started (Two-phase commitment protocol)");
		
		for (int i = 0; i < logs.size(); i++) {
			Log log = logs.get(i);
			
			if (log.operation() != Log.Operation.GET) {
				Object entity = log.entity();
				ResourceManager<Transaction> manager = managers.get(keyValue(entity));
				
				try {
					if (i == last) {
						List<Entity> entities = Translation.toEntities(transaction,
								Keys.create(Translation.toKind(entity.getClass()), 
										keyValue(entity)));
						datastore.put(manager.transaction(), entities);
					}
					
					manager.prepare();
					log.state(State.PREPARED);
				} catch (Exception e) {
					throw new UncheckedException(e);
				}
			}
		}
		
		logger.fine("Transaction [" + transaction.id() + "]: Transaction has been prepared");
		
		// Phase 2: Commitment phase.
		for (Log log : logs) {
			if (log.operation() != Log.Operation.GET) {
				ResourceManager<Transaction> manager = managers.get(keyValue(log.entity()));
				
				try {
					manager.commit();
					log.state(State.COMMITTED);
				} catch (Exception e) {
					logger.warning("Transaction [" + transaction.id()
							+ "]: Commitment failed due to [" + e
							+ "]; Consistency will be ensured in read");
					e.printStackTrace();
					return;
				}
			}
		}
		
		logger.fine("Transaction [" + transaction.id() + "]: Transaction has been committed");
	}

	/** Rolls back every operation in the current transaction. */
	@Override
	public void rollback() {
		for (ResourceManager<Transaction> manager : managers.values()) {
			Transaction transaction = manager.transaction();
			
			if (transaction.isActive()) {
				try {
					transaction.rollback();
				} catch (Exception e) {
					logger.warning("Transaction [" + transaction.getId()
							+ "]: Rollback failed due to [" + e
							+ "]; Consistency will be ensured in read");
				}
			}
		}
	}

}
