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

/**
 * {@code Coordinator} is a transaction coordinator for "Two-phase commit protocol".
 * {@code Coordinator} is instantiated by {@code Session} when a 
 * transaction is started and every transactional operation is forwarded to the 
 * coordinator. If multiple entities are updated (also, inserted and deleted) in 
 * a transaction, the coordinator has to make multiple {@code ResourceManager}s 
 * manage them and commit these operations atomically by 
 * <a href="http://en.wikipedia.org/wiki/Two-phase_commit_protocol">Two-phase commit protocol</a>.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public interface Coordinator {

	/**
	 * Gets entity instance of the specified {@code Class} corresponding to the 
	 * specified id in the current transaction.
	 * 
	 * @param <E> The type of entity.
	 * @param clazz The {@code Class} of entity.
	 * @param key The key of the entity.
	 * @return Entity instance of the specified {@code Class} corresponding to 
	 * the specified entity id.
	 */
	public <E> E get(Class<E> clazz, Object key);
	
	/**
	 * Puts entity instance in the current transaction.
	 * 
	 * @param entity The entity to be put into datastore.
	 */
	public void put(Object entity);
	
	/**
	 * Updates entity instance in the current transaction.
	 * 
	 * @param entity The entity to be updated.
	 */
	public void update(Object entity);
	
	/**
	 * Deletes entity instance in the current transaction.
	 * 
	 * @param entity The entity to be deleted.
	 */
	public void delete(Object entity);
	
	/**
	 * Commits every operation in the current transaction with Two-phase commit 
	 * protocol.
	 * 
	 * @throws IndoubtException If the data consistency is broken.
	 */
	public void commit() throws IndoubtException;
	
	/**
	 * Rolls back every operation in the current transaction.
	 */
	public void rollback();
	
}
