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

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eiichiro.reverb.lang.UncheckedException;

/**
 * {@code Property} is the metamodel of Acid House entity's property.
 * This is called "Metamodel property" and consists of the type of property, the 
 * name of property, the {@code Metamodel} instance which has this metamodel 
 * property and {@code String} path representation to this metamodel property.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Property<E, T> {

	private final Metamodel<E> metamodel;
	
	private final Property<E, ?> parent;
	
	private final Type type;
	
	private final String name;
	
	private final List<String> path;
	
	/**
	 * Returns the property name which this metamodel property represents.
	 * 
	 * @return The property name which this metamodel property represents.
	 */
	public String name() {
		return name;
	}

	/**
	 * Constructs a new {@code Property} instance with the specified 
	 * {@code Metamodel}, property type and property name.
	 * 
	 * @param metamodel The {@code Metamodel} instance which has this metamodel 
	 * property.
	 * @param type The property type.
	 * @param name The property name.
	 */
	public Property(Metamodel<E> metamodel, Type type, String name) {
		this.metamodel = metamodel;
		parent = null;
		this.type = type;
		this.name = name;
		path = path(metamodel, name);
	}
	
	/**
	 * Constructs a new {@code Property} instance with the specified 
	 * {@code EmbeddedProperty}, property type and property name.
	 * 
	 * @param parent The parent {@code Property} instance which has this metamodel 
	 * property.
	 * @param type The property type.
	 * @param name The property name.
	 */
	public Property(Property<E, ?> parent, Type type, String name) {
		this.metamodel = parent.metamodel();
		this.parent = parent;
		this.type = type;
		this.name = name;
		path = path(metamodel, name, parent.name());
	}
	
	/**
	 * Returns the {@code Metamodel} instance which has this metamodel 
	 * property.
	 * 
	 * @return The {@code Metamodel} instance which has this metamodel property.
	 */
	public Metamodel<E> metamodel() {
		return metamodel;
	}
	
	/**
	 * Returns the property value which this metamodel property instance 
	 * represents from the specified entity instance.
	 * 
	 * @param entity The entity instance from which you attempt to get the 
	 * property value. <b>The specified entity have to make sure to be JavaBeans
	 * to get property value from it.</b>
	 * @return The property value of the specified entity instance.
	 */
	@SuppressWarnings("unchecked")
	public T get(Object entity) {
		Object value = entity;
		
		for (String name : path()) {
			try {
				Field field = value.getClass().getDeclaredField(name);
				field.setAccessible(true);
				value = field.get(value);
			} catch (Exception e) {
				throw new UncheckedException(e);
			}
		}
		
		return (T) value;
	}
	
	/**
	 * Sets the property value which this metamodel property instance 
	 * represents to the specified entity instance.
	 * 
	 * @param entity The entity instance into which you attempt to set the 
	 * property value. <b>The specified entity have to make sure to be JavaBeans
	 * to set property value into it.</b>
	 * @param value The property value to be set into entity instance.
	 */
	public void set(E entity, Object value) {
		Object object = entity;
		
		for (int i = 0; i < path().size() - 1; i++) {
			try {
				Field field = object.getClass().getDeclaredField(path().get(i));
				field.setAccessible(true);
				object = field.get(object);
			} catch (Exception e) {
				throw new UncheckedException(e);
			}
		}
		
		try {
			Field field = object.getClass().getDeclaredField(name);
			field.setAccessible(true);
			field.set(object, value);
		} catch (Exception e) {
			throw new UncheckedException(e);
		}
	}
	
	/**
	 * Returns the parent {@code Property} instance which has this metamodel 
	 * property.
	 * 
	 * @return The parent {@code Property} instance which has this metamodel 
	 * property.
	 */
	public Property<E, ?> parent() {
		return parent;
	}

	/**
	 * Returns the property type which this metamodel property represents.
	 * 
	 * @return The property type which this metamodel property represents.
	 */
	public Type type() {
		return type;
	}
	
	/**
	 * Returns {@code String} path representation to this property.
	 * 
	 * @return The {@code String} path representation to this property.
	 */
	public List<String> path() {
		return path;
	}
	
	private List<String> path(Metamodel<E> metamodel, String... names) {
		List<String> path = new ArrayList<String>();
		
		for (String name : names) {
			path.add(name);
		}
		
		if (!metamodel.isRoot()) {
			path.add(metamodel.name());
		}
		
		Metamodel<?> parent = metamodel.parent();
		
		while (parent != null && !parent.isRoot()) {
			path.add(parent.name());
			parent = parent.parent();
		}
		
		Collections.reverse(path);
		return path;
	}
	
}
