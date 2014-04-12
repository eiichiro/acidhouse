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

/**
 * {@code ResourceManager} manages an entity instance, datastore connection 
 * corresponding to the entity and its transactional state.
 * {@code ResourceManager} is instantiated and invoked by {@code Session} 
 * and {@code Coordinator} (especially, in distributed transaction), and applies 
 * datastore operation to the managed entity with the datastore-specific APIs.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public interface ResourceManager<T> {

	/**
	 * Gets the entity instance of the specified {@code Class} corresponding to 
	 * the specified id.
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
	public <E> E get(Class<E> clazz, Object key) throws ConcurrentModificationException, IndoubtException;
	
	/**
	 * Puts the specified entity instance into the datastore newly.
	 * 
	 * @param entity An entity instance.
	 */
	public void put(Object entity);
	
	/**
	 * Applies the specified entity's update to the datastore.
	 *  
	 * @param entity An entity instance to be updated.
	 */
	public void update(Object entity);
	
	/**
	 * Deletes the specified entity from the datastore.
	 * 
	 * @param entity An entity instance to be updated.
	 */
	public void delete(Object entity);
	
	/**
	 * Allocates lock for the managed entity.
	 * This operation is the preparation for the current transaction's commitment, 
	 * and invoked by {@code Coordinator} in Two-phase commit protocol.
	 */
	public void prepare();
	
	/**
	 * Applies the transactional operation (WAL: Write Ahead Log) to the managed 
	 * entity then frees the allocated lock.
	 * This operation is the completion for the current transaction's commitment, 
	 * and incoked by {@code Coordinator} in Two-phase commit protocol.
	 */
	public void commit();
	
	/**
	 * Returns the managed entity instance.
	 * 
	 * @return The managed entity instance.
	 */
	public Object entity();
	
	/**
	 * Returns the current transaction.
	 * If the managed entity is out of transaction, this method returns 
	 * {@code null}.
	 * 
	 * @return The current transaction.
	 */
	public T transaction();
	
}
