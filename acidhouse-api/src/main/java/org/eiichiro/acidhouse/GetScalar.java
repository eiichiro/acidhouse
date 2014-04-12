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
 * {@code GetScalar} is a command interface to get the scalar value as the 
 * result for aggregating of entity list matches to the specified 
 * {@code Filter}s and sorted by the specified sort orders.
 * This command's execution result is the returned value of the specified 
 * {@code Aggregation} instance's {@code Aggregation#aggregate(java.util.List)} 
 * method.
 * You can build &amp; execute this command as the following code, in App 
 * Engine: 
 * <pre>
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Create 'Session' instance.
 * Session session = new AppEngineDatastoreSession();
 * // Get metamodel instance of 'Entity3' class.
 * Entity3$ entity3$ = Metamodels.metamodel(Entity3.class);
 * // Get summary of 'i' field of the 'Entity3' entities which their 'i' field 
 * // is less than 5.
 * int sum = session
 * 		.get(Aggregations.sum(entity3$.i))
 * 		.filter(entity3$.i.lessThan(5))
 * 		.execute();
 * </pre>
 * 
 * @see Aggregation
 * @see Aggregations
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public interface GetScalar<E, R> extends Command<R> {

	/**
	 * Specifies sort orders by which the list passed to 
	 * {@code Aggregation#aggregate(java.util.List)} method is sorted.
	 * 
	 * @param orders The sort orders by which the returned list is sorted.
	 * @return The {@code GetScalar} which the execution result passed to 
	 * {@code Aggregation#aggregate(java.util.List)} is sorted 
	 * by the specified sort orders.
	 */
	public GetScalar<E, R> sort(Order<?, ?>... orders);

	/**
	 * Qualifies entities to be retrieved with the specified {@code Filter}s.
	 * Each of the specified filters is combined with "logical and".
	 * 
	 * @param filters {@code Filter}s to qualify the retrieved entities.
	 * @return The {@code GetScalar} which the execution result passed to 
	 * {@code Aggregation#aggregate(java.util.List)} is qualified by the 
	 * specified filters.
	 */
	public GetScalar<E, R> filter(Filter<?>... filters);
	
}
