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

import org.eiichiro.acidhouse.metamodel.Property;

/**
 * {@code Filter} qualifies the entities to which a {@code Command} is applied.
 * You can get {@code Filter} instances from metamodel property and can pass to 
 * {@code GetList}, {@code GetScalar}, {@code Update} and {@code Delete}'s 
 * {@code filter(Filter...)} method. 
 * As an example in App Engine: 
 * <pre>
 * import org.eiichiro.acidhouse.Session;
 * import org.eiichiro.acidhouse.appengine.AppEngineDatastoreSession;
 * ...
 * 
 * // Creating 'Session' instance.
 * Session session = new AppEngineDatastoreSession();
 * // Getting metamodel instance of 'Entity3' class.
 * Entity3_ entity3_ = Metamodels.metamodel(Entity3.class);
 * // Qualifying entities which their 'entity1.i' proerty is greater than or 
 * // equal to 13, and 15 or 16.
 * List&lt;Entity3&gt; entity3s = session
 * 		.get(entity3_)
 * 		.filter(entity3_.entity1.i.greaterThanOrEqualTo(13), 
 * 			entity3_.entity1.i.in(15, 16))
 * 		.execute();
 * </pre>
 * 
 * @see ComparableProperty
 * @see Metamodel
 * @see GetList
 * @see GetScalar
 * @see Update
 * @see Delete
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public interface Filter<T> {

	/**
	 * Returns the metamodel {@code Property} this {@code Filter} is applied to.
	 * 
	 * @return The metamodel {@code Property} this {@code Filter} is applied to.
	 */
	public Property<?, T> property();
	
	/**
	 * Indicates whether the specified entity matches to this {@code Filter} or 
	 * not. This method is invoked by {@code Command} implementation for 
	 * in-memory filtering.
	 * 
	 * @param entity The entity to be filtered.
	 * @return Whether the specified entity matches to this {@code Filter} or 
	 * not.
	 */
	public boolean matches(Object entity);
	
}
