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
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.Entities;
import org.eiichiro.acidhouse.EntityExistsException;
import org.eiichiro.acidhouse.IndoubtException;
import org.eiichiro.acidhouse.Lock;
import org.eiichiro.acidhouse.Log;
import org.eiichiro.acidhouse.Log.State;
import org.eiichiro.acidhouse.ResourceManager;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

/**
 * {@code AppEngineResourceManager} is a App Engine Low-level Datastore API 
 * based {@code ResourceManager} implementation.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineResourceManager implements ResourceManager<Transaction> {

	private final Logger logger = Logger.getLogger(getClass().getName());
	
	private final AppEngineDatastoreService datastore;
	
	private final Transaction local;
	
	private final AppEngineGlobalTransaction global;
	
	private Object entity;
	
	/**
	 * Constructs a new {@code AppEngineResourceManager} with the specified 
	 * {@code AppEngineDatastoreService}.
	 * 
	 * @param datastoreService {@code AppEngineDatastoreService}.
	 */
	public AppEngineResourceManager(AppEngineDatastoreService datastoreService) {
		this(datastoreService, null, null);
	}
	
	/**
	 * Constructs a new {@code AppEngineResourceManager} with the specified 
	 * {@code AppEngineDatastoreService}, Google App Engine Datastore local 
	 * {@code Transaction} and Acid House {@code AppEngineGlobalTransaction}.
	 * 
	 * @param datastore {@code AppEngineDatastoreService}.
	 * @param local Google App Engine Datastore local {@code Transaction}.
	 * @param global Acid House {@code AppEngineGlobalTransaction}.
	 */
	public AppEngineResourceManager(AppEngineDatastoreService datastore,
			Transaction local, AppEngineGlobalTransaction global) {
		this.datastore = datastore;
		this.local = local;
		this.global = global;
	}
	
	/**
	 * Gets the entity instance of the specified {@code Class} corresponding to 
	 * the specified key.
	 * This method implements "Consistent read". This method gets the entity as 
	 * the following procedure.
	 * <ul>
	 * <li>
	 * Determines if the entity is locked ({@code Lock} entity is found). If the 
	 * entity has been locked, goes to the next step. If the entity has not been 
	 * locked, jumps to the last step.
	 * </li>
	 * <li>
	 * Attempts to get {@code AppEngineGlobalTransaction} from {@code Lock}. If the 
	 * {@code AppEngineGlobalTransaction} is found, goes to the next step. If the 
	 * {@code AppEngineGlobalTransaction} is not found and transaction timeout has 
	 * been elapsed, this method determines the transaction had been failed 
	 * (rolled back), then deletes {@code AppEngineGlobalTransaction} from Google App 
	 * Engine Datastore and goes to the last step. If the {@code AppEngineGlobalTransaction} 
	 * is not found and transaction timeout has not been elapsed, this method 
	 * determines the entity is being modified under a transaction and throws 
	 * {@code ConcurrentModificationException}.
	 * </li>
	 * <li>
	 * Gets the list of {@code Log} from {@code AppEngineGlobalTransaction} and 
	 * applies every operation to entities, then unlocks them.
	 * </li>
	 * <li>
	 * Gets the entity from Google App Engine Datastore and checks the entity 
	 * is locked again. if the entity has been locked, this method throws 
	 * {@code ConcurrentModificationException}. Otherwise, this method returns 
	 * the entity.
	 * </li>
	 * </ul>
	 * 
	 * @param <E> The type of entity.
	 * @param clazz The {@code Class} of entity.
	 * @param key The key corresponding to the entity you attempt to get.
	 * @return The entity instance of the specified {@code Class} corresponding 
	 * to the specified key.
	 * @throws ConcurrentModificationException If the entity has been modified 
	 * by other transaction.
	 * @throws IndoubtException If the data consistency broken is detected when 
	 * the entity is get.
	 */
	@Override
	public <E> E get(Class<E> clazz, Object key)
			throws ConcurrentModificationException, IndoubtException {
		Key k = Keys.create(Translation.toKind(clazz), key);
		unlock(k);
		List<Entity> entities = null;
		
		if ((local == null)) {
			entities = datastore.query(new Query(k));
		} else {
			entities = datastore.query(local, new Query(k));
		}
		
		for (Entity entity : entities) {
			if (entity.getKind().equals(Translation.LOCK_KIND)) {
				throw new ConcurrentModificationException(
						"Entity corresponding to [" + k + "] is processed under a transaction");
			}
		}
		
		E object = Translation.toObject(clazz, entities, new HashMap<Key, Object>(), datastore);
		entity = object;
		operation = Log.Operation.GET;
		return object;
	}

	private void unlock(Key key) {
		List<Entity> entities = datastore.query(new Query(Translation.LOCK_KIND, key));
		
		// If the entity corresponding to the specified key hasn't been locked, 
		// this method ignores it.
		if (entities.size() == 0) {
			return;
		}
		
		Entity lockEntity = entities.get(0);
		Lock lock = Translation.toLock(lockEntity);
		
		// If transaction entities are not found, this method determines that the 
		// entity corresponding to the specified key is processed under a 
		// transaction when the current time within the deadline of datastore 
		// operation. When the current time has exceeded the deadline of 
		// datastore operation, this method determines that the another 
		// transaction has failed and so just unlocks the entity.
		Key transactionKey = KeyFactory.stringToKey(lock.transaction());
		List<Entity> transactionEntities = new ArrayList<Entity>();
		
		if (datastore.get(transactionKey) == null) {
			if (System.currentTimeMillis() - lock.timestamp().getTime() <= datastore.deadline()) {
				throw new ConcurrentModificationException(
						"Entity corresponding to [" + key + "] is processed under a transaction");
			} else {
				Transaction transaction = datastore.beginTransaction();
				
				if (datastore.get(transaction, lockEntity.getKey()) == null) {
					logger.info("Entity locked by [" + lock.id()
							+ "] has been rollbacked by another transaction");
					transaction.rollback();
					return;
				}
				
				datastore.delete(transaction, lockEntity.getKey());
				transaction.commit();
				return;
			}
			
		} else {
			transactionEntities.addAll(datastore.query(new Query(transactionKey)));
		}
		
		AppEngineGlobalTransaction transaction 
				= Translation.toTransaction(transactionEntities, datastore);
		Key ancestor = Keys.ancestor(transactionKey);
		
		Log owner = null;
		Key ownerLockKey = null;
		int committed = 0;
		
		for (Log log : transaction.logs()) {
			Key entityKey = Keys.create(
					Translation.toKind(log.entity().getClass()), 
					Entities.keyValue(log.entity()));
			Key lockKey = Keys.create(entityKey, Translation.LOCK_KIND, lock.id());
			
			// The owner entity of transaction entities must be deleted after 
			// every operation has been applied. Because this method cannot 
			// apply any operation to entity without transaction entities.
			if (entityKey.equals(ancestor)) {
				owner = log;
				ownerLockKey = lockKey;
				continue;
			}
			
			List<Key> deletes = new ArrayList<Key>();
			deletes.add(lockKey);
			
			// Applies operation to entity and unlocks it.
			try {
				apply(log.operation(), log.entity(), lockKey, deletes);
				log.state(State.COMMITTED);
				committed++;
			} catch (Exception e) {
				throwIndoubtException(e, transaction, committed);
			}
		}
		
		// At last, this method applies operation to the owner entity, unlocks 
		// it and deletes transaction entities.
		if (owner != null) {
			List<Key> deletes = new ArrayList<Key>();
			deletes.add(ownerLockKey);
			
			for (Entity delete : transactionEntities) {
				deletes.add(delete.getKey());
			}
			
			try {
				apply(owner.operation(), owner.entity(), ownerLockKey, deletes);
				owner.state(State.COMMITTED);
				committed++;
			} catch (Exception e) {
				throwIndoubtException(e, transaction, committed);
			}
		}
		
		// Done!
	}
	
	private void apply(Log.Operation operation, Object entity, Key lock, List<Key> deletes) {
		Transaction transaction = datastore.beginTransaction();
		
		if (datastore.get(transaction, lock) == null) {
			logger.info("Entity locked by [" + lock
					+ "] has been applied by another transaction");
			transaction.rollback();
			return;
		}
		
		if (operation != Log.Operation.DELETE) {
			datastore.put(transaction, Translation.toEntities(entity));
		} else {
			for (Entity e : Translation.toEntities(entity)) {
				deletes.add(e.getKey());
			}
		}
		
		datastore.delete(transaction, deletes);
		transaction.commit();
	}
	
	private void throwIndoubtException(Exception exception,
			AppEngineGlobalTransaction transaction, int committed) {
		logger.severe("Transaction [" 
				+ transaction.id() 
				+ "]: Failed to apply transaction log; " 
				+ "DATA CONSISTENCY BROKEN! PLEASE RECOVERY IMMEDIATELY " 
				+ "WITH THE FOLLOWING TRANSACTION DUMP :-<");
		throw new IndoubtException(exception, transaction.id(), transaction.logs());
	}

	/**
	 * Puts the specified entity instance into the Google App Engine Datastore 
	 * newly.
	 * 
	 * @param entity An entity instance.
	 */
	@Override
	public void put(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' must not be [" + entity + "]");
		}
		
		if (operation != null) {
			throw new IllegalStateException("Log [" + operation + "] -> ["
					+ Log.Operation.PUT
					+ "] is not allowed: This operation must be first");
		}
		
		Key id = Keys.create(Translation.toKind(entity.getClass()), Entities.keyValue(entity));
		Entity e = datastore.get(local, id);
		
		if (e != null) {
			throw new EntityExistsException(id);
		}
		
		this.entity = entity;
		operation = Log.Operation.PUT;
	}

	/**
	 * Applies the specified entity's update to the Google App Engine Datastore.
	 *  
	 * @param entity An entity instance to be updated to.
	 */
	@Override
	public void update(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' must not be [" + entity + "]");
		}
		
		if (operation != Log.Operation.GET) {
			throw new IllegalStateException("Log [" + operation + "] -> ["
					+ Log.Operation.UPDATE
					+ "] is not allowed: previous operation must be ["
					+ Log.Operation.GET + "]");
		}
		
		operation = Log.Operation.UPDATE;
	}

	/**
	 * Deletes the specified entity from the Google App Engine Datastore.
	 * 
	 * @param entity An entity instance to be deleted.
	 */
	@Override
	public void delete(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("'entity' must not be [" + entity + "]");
		}
		
		if (operation != Log.Operation.GET) {
			throw new IllegalStateException("Log [" + operation + "] -> ["
					+ Log.Operation.DELETE
					+ "] is not allowed: previous operation must be ["
					+ Log.Operation.GET + "]");
		}
		
		operation = Log.Operation.DELETE;
	}

	/** Allocates lock for the managing entity. */
	@Override
	public void prepare() {
		List<Log> logs = global.logs();
		Object parent = null;
		
		for (int i = logs.size() - 1; i >=0; i--) {
			if (logs.get(i).operation() != Log.Operation.GET) {
				parent = logs.get(i).entity();
				break;
			}
		}
		
		Lock lock = new Lock(global.id(), KeyFactory.createKeyString(
				Keys.create(Translation.toKind(parent.getClass()), Entities.keyValue(parent)), 
				Translation.TRANSACTION_KIND, 
				global.id()), new Date());
		datastore.put(local, Translation.toEntity(lock, Keys.create(
				Translation.toKind(entity.getClass()), Entities.keyValue(entity))));
		local.commit();
		this.lock = lock;
	}

	/**
	 * Applies the transactional operation (WAL: Write Ahead Log) to the 
	 * managing entity and unlocks it. 
	 */
	@Override
	public void commit() {
		List<Key> deletes = new ArrayList<Key>();
		deletes.add(Translation.toEntity(lock, Keys.create(
				Translation.toKind(entity.getClass()), 
				Entities.keyValue(entity))).getKey());
		Key transactionKey = KeyFactory.stringToKey(lock.transaction());
		
		if (Keys.ancestor(transactionKey).equals(
				Keys.create(Translation.toKind(entity.getClass()), Entities.keyValue(entity)))) {
			deletes.add(transactionKey);
			
			for (Entity entity : datastore.query(
					new Query(transactionKey).setKeysOnly())) {
				deletes.add(entity.getKey());
			}
		}
		
		Key parent = Keys.create(Translation.toKind(entity.getClass()), Entities.keyValue(entity));
		apply(operation, entity, Translation.toEntity(lock, parent).getKey(), deletes);
	}

	private Log.Operation operation;
	
	private Lock lock;
	
	/**
	 * Returns the entity this {@code AppEngineResourceManager} manages.
	 * 
	 * @return The entity this {@code AppEngineResourceManager} manages.
	 */
	@Override
	public Object entity() {
		return entity;
	}

	/**
	 * Returns the {@code Transaction} this {@code AppEngineResourceManager} 
	 * manages.
	 * 
	 * @return The {@code Transaction} this {@code AppEngineResourceManager} 
	 * manages.
	 */
	@Override
	public Transaction transaction() {
		return local;
	}

}
