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

import java.util.List;

/**
 * {@code GetList} is a command interface to get entities match to the 
 * specified {@code Filter}s in the specified range ordered by the specified 
 * sort orders. This command's execution result is entities as {@code List} 
 * view. You can build &amp; execute this command as the following code, in App 
 * Engine example: 
 * <pre>
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Creating 'Session' instance.
 * Session session = new AppEngineDatastoreSession();
 * // Get metamodel instance of 'Entity3' class.
 * Entity3$ entity3$ = Metamodels.metamodel(Entity3.class);
 * // Filtering: "Entity3"'s 'entity1.i' property is greater than or equal to 13, 
 * // and 15 or 16 or 17 or 18 or 19 or 20.
 * // Range: from 1 to 3 (offset value).
 * // Ordering: In descending order of "Entity3"'s 'i' property.
 * List&lt;Entity3&gt; entity3s = session
 * 		.get(entity3$)
 * 		.filter(entity3$.entity1.i.greaterThanOrEqualTo(13), 
 * 			entity3$.entity1.i.in(15, 16, 17, 18, 19, 20))
 * 		.offset(1)
 * 		.limit(3)
 * 		.sort(entity3$.i.desc)
 * 		.execute();
 * </pre>
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public interface GetList<E> extends Command<List<E>> {

	/**
	 * Specifies sort orders by which the returned list is sorted.
	 * 
	 * @param orders The sort orders by which the returned list is sorted.
	 * @return The {@code GetList} which the execution result is sorted 
	 * by the specified sort orders.
	 */
	public GetList<E> sort(Order<?, ?>... orders);
	
	/**
	 * Qualifies entities to be retrieved with the specified {@code Filter}s.
	 * Each of the specified filters is combined with "logical and".
	 * 
	 * @param filters {@code Filter}s to qualify entities to be retrieved.
	 * @return The {@code GetList} which the execution result is 
	 * qualified with the specified {@code Filter}s.
	 */
	public GetList<E> filter(Filter<?>... filters);
	
	/**
	 * Qualifies limit size of returned list.
	 * 
	 * @param limit The limit size of returned list.
	 * @return The {@code GetList} which the execution result is qualified with 
	 * the specified limit.
	 */
	public GetList<E> limit(int limit);
	
	/**
	 * Qualifies offset of execution result to be contained in the returned list 
	 * at first.
	 * 
	 * @param offset The offset of execution result to be contained in the 
	 * returned list at first.
	 * @return The {@code GetList} which the execution result is qualified with 
	 * the specified offset.
	 */
	public GetList<E> offset(int offset);
	
}
