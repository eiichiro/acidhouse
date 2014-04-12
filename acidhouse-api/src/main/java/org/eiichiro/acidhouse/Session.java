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

import java.util.ConcurrentModificationException;

import org.eiichiro.acidhouse.metamodel.Metamodel;

/**
 * {@code Session} is the central client-interfaced object to control 
 * NoSQL datastore on Acid House and represents interactive session between 
 * client and NoSQL datastore.
 * Basic CRUD operations are like this, in App Engine example: 
 * <pre>
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Creating entity.
 * Entity1 entity11 = new Entity1();
 * entity11.id = "Key11";
 * entity11.i = 11;
 * Entity1 entity12 = new Entity1();
 * entity12.id = "Key12";
 * entity12.i = 12;
 * Session session = new AppEngineDatastoreSession();
 * Transaction transaction = session.beginTransaction();
 * 
 * try {
 * 	session.put(entity11);
 * 	session.put(entity12);
 * 	transaction.commit();
 * } catch (Exception e) {
 * 	transaction.rollback();
 * 	throw e;
 * } finally {
 * 	session.close();
 * }
 * 
 * 
 * // Reading entity.
 * Session session = new AppEngineDatastoreSession();
 * Entity1 entity11 = session.get(Entity1.class, "Key11");
 * Entity1 entity12 = session.get(Entity1.class, "Key12");
 * session.close();
 * 
 * 
 * // Updating entity.
 * Session session = new AppEngineDatastoreSession();
 * Transaction transaction = session.beginTransaction();
 * 
 * try {
 * 	Entity1 entity11 = session.get(Entity1.class, "Key11");
 * 	Entity1 entity12 = session.get(Entity1.class, "Key12");
 * 	entity11.i = 1111;
 * 	entity12.i = 1222;
 * 	session.update(entity11);
 * 	session.update(entity12);
 * 	transaction.commit();
 * } catch (Exception e) {
 * 	transaction.rollback();
 * 	throw e;
 * } finally {
 * 	session.close();
 * }
 * 
 * 
 * // Deleting entity.
 * Session session = new AppEngineDatastoreSession();
 * Transaction transaction = session.beginTransaction();
 * 
 * try {
 * 	Entity1 entity11 = session.get(Entity1.class, "Key11");
 * 	Entity1 entity12 = session.get(Entity1.class, "Key12");
 * 	session.delete(entity11);
 * 	session.delete(entity12);
 * 	transaction.commit();
 * } catch (Exception e) {
 * 	transaction.rollback();
 * 	throw e;
 * } finally {
 * 	session.close();
 * }
 * </pre>
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public interface Session {

	/**
	 * Begins transaction.
	 * You have to do transactional operation after calling this method.
	 * 
	 * @return The transaction begun newly.
	 */
	public Transaction beginTransaction();
	
	/** Closes this session. */
	public void close();
	
	// CRUD operation.
	
	/**
	 * Returns the entity instance corresponding to the specified key.
	 * It depends on the implementation of this interface whether an exception 
	 * is thrown or {@code null} is returned if the entity is not found.
	 * 
	 * @param <E> The entity type.
	 * @param clazz The entity type that you want to get.
	 * @param key The key corresponding to the entity that you want to get.
	 * @return The entity instance corresponding to the specified key.
	 * @throws ConcurrentModificationException If the entity corresponding to 
	 * the specified key is being modified by the other transaction.
	 */
	public <E> E get(Class<E> clazz, Object key) throws ConcurrentModificationException;
	
	/**
	 * Puts the specified entity instance into datastore newly.
	 * This method must be invoked under a transaction. Depending on the 
	 * implementation of this interface, if the entity that has the same key as 
	 * the specified entity has been already stored, this method might throw 
	 * {@code EntityExistsException}.
	 * 
	 * @param entity The entity instance to be put into Session.
	 * @throws EntityExistsException If the entity that has the same key as the 
	 * specified entity has already existed.
	 */
	public void put(Object entity) throws EntityExistsException;
	
	/**
	 * Updates entity with the specified entity instance.
	 * This method must be invoked under a transaction. 
	 * 
	 * @param entity The entity instance to be updated.
	 */
	public void update(Object entity);
	
	/**
	 * Deletes the specified entity from datastore.
	 * This method must be invoked under a transaction.
	 * 
	 * @param entity The entity to be deleted.
	 */
	public void delete(Object entity);
	
	// Command Builder API.
	
	/**
	 * Returns {@code GetList} for the specified entity metamodel.
	 * This method is the entry point for getting-list Command Builder API.
	 * 
	 * @param <E> The entity type to get with this {@code GetList}.
	 * @param metamodel The metamodel of the entity to get with this 
	 * {@code GetList}. 
	 * @return {@code GetList} for the specified entity class.
	 */
	public <E> GetList<E> get(Metamodel<E> metamodel);
	
	/**
	 * Returns {@code GetScalar} for the specified {@code Aggregation}.
	 * This method is the entry point for aggregation Command Builder API.
	 * 
	 * @param <R> The property value type to aggregate with this 
	 * {@code GetScalar}.
	 * @param aggregation The {@code Aggregation} this method executes.
	 * @return {@code GetScalar} for the specified {@code Aggregation} 
	 * instance.
	 */
	public <E, R> GetScalar<E, R> get(Aggregation<R> aggregation);
	
	/**
	 * Returns {@code Update} for the specified entity metamodel.
	 * This method is the entry point for updating Command Builder API.
	 * 
	 * @param <E> The type of entity updated by this {@code Update}.
	 * @param metamodel The metamodel of the entity updated by this 
	 * {@code Update}.
	 * @return The {@code Update} for the specified entity class.
	 */
	public <E> Update<E> update(Metamodel<E> metamodel);
	
	/**
	 * Returns {@code Delete} for the specified entity metamodel.
	 * This method is the entry point for deleting Command Builder API.
	 * 
	 * @param <E> The type of entity deleted by this {@code Delete}.
	 * @param metamodel The metamodel of the entity deleted by this 
	 * {@code Delete}.
	 * @return The {@code Delete} for the specified entity class.
	 */
	public <E> Delete<E> delete(Metamodel<E> metamodel);
	
}
