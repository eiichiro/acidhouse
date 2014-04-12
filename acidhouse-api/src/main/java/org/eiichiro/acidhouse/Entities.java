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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eiichiro.reverb.lang.UncheckedException;

/**
 * {@code Entities} is a utility class to provide convenient methods for 
 * Acid House entity.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Entities {

	private Entities() {}
	
	private static Map<Class<?>, Field> fields = new ConcurrentHashMap<Class<?>, Field>();
	
	/**
	 * Returns key value of the specified entity instance.
	 * 
	 * @param entity An entity instance.
	 * @return The id value of the specified entity instance.
	 */
	public static Object keyValue(Object entity) {
		Class<?> clazz = entity.getClass();
		Field key = fields.get(clazz);
		
		try {
			if (key == null) {
				return keyField(clazz).get(entity);
			} else {
				return key.get(entity);
			}
			
		} catch (IllegalAccessException e) {
			throw new UncheckedException(e);
		}
	}
	
	/**
	 * Returns key field of the specified entity class.
	 * 
	 * @param clazz An entity class.
	 * @return The key field of the specified entity class.
	 */
	public static Field keyField(Class<?> clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(Key.class) != null) {
				field.setAccessible(true);
				fields.put(clazz, field);
				return field;
			}
		}
		
		throw new IllegalArgumentException("Entity [" + clazz
				+ "] must have one @org.eiichiro.acidhouse.Key field");
	}
	
}
