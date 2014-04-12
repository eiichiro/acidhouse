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
 * {@code Delete} is a command interface to delete entities match to the 
 * specified {@code Filter}s in Acid House's transactional session. This 
 * command execution returns the count of deleted entities. You can build &amp; 
 * execute this command as the following code, in App Engine example: 
 * <pre>
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Creating 'Session' instance.
 * Session session = new AppEngineDatastoreSession();
 * // Beginning transaction.
 * Transaction transaction = session.beginTransaction();
 * // Getting metamodel instance of 'Entity3' class.
 * Entity3$ entity3$ = Metamodels.metamodel(Entity3.class);
 * 
 * try {
 * 	// Deleting all of 'Entity3' entities which their 'entity1.i' property value 
 *  // is less than 15.
 * 	session.delete(entity3$)
 * 			.filter(entity3$.entity1.i.lessThan(15))
 * 			.execute();
 * 	// Committing transaction.
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
public interface Delete<E> extends Command<Integer> {

	/**
	 * Qualifies entities to be deleted with the specified {@code Filter}s.
	 * Each of the specified filters is combined with "logical and".
	 * 
	 * @param filters {@code Filter}s to qualify entities to be deleted.
	 * @return The {@code Delete} to delete entities qualified with the 
	 * specified {@code Filter}s.
	 */
	public Delete<E> filter(Filter<?>... filters);
	
}
