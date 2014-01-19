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
package org.eiichiro.acidhouse.metamodel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eiichiro.reverb.lang.UncheckedException;

/**
 * {@code Metamodels} provides some {@code Metamodel} control methods.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public abstract class Metamodels {

	private static Map<Class<?>, Metamodel<?>> metamodels 
			= new ConcurrentHashMap<Class<?>, Metamodel<?>>();
	
	private Metamodels() {}
	
	/**
	* Creates {@code Metamodel} instance corresponding to the specified entity 
	* class.
	* {@code Metamodel} class have to be declared as the name that adding '_' 
	* prefix to the entity class name in the same package as the entity class.
	* 
	* @param <M> The type of metamodel class.
	* @param <E> The type of entity class.
	* @param entity The entity from which metamodel instance is created.
	* @return The {@code Metamodel} instance corresponding to the specified 
	* entity class.
	*/
	@SuppressWarnings("unchecked")
	public static <M extends Metamodel<E>, E> M metamodel(Class<E> entity) {
		if (metamodels.containsKey(entity)) {
			return (M) metamodels.get(entity);
		} else {
			try {
				int i = entity.getName().lastIndexOf(".");
				M metamodel = null;
				
				if (i < 0) {
					metamodel = (M) Class.forName("_" + entity.getName()).newInstance();
				} else {
					metamodel = (M) Class.forName(
							entity.getName().substring(0, entity.getName().lastIndexOf(".") + 1) 
							+ "_" 
							+ entity.getName().substring(entity.getName().lastIndexOf(".") + 1)).newInstance();
				}
				
				metamodels.put(entity, metamodel);
				return metamodel;
			} catch (Exception e) {
				throw new UncheckedException(e);
			}
		}
	}
	
}
