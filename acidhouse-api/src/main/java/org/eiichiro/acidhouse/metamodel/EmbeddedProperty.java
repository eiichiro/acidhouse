/*
 * Copyright (C) 2012 Eiichiro Uchiumi. All Rights Reserved.
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

import java.lang.reflect.Type;

import org.eiichiro.acidhouse.Embedded;

/**
 * {@code EmbeddedProperty} represents a metamodel of {@link Embedded} value 
 * entity property.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public abstract class EmbeddedProperty<E, T> extends Property<E, T> {

	/**
	 * Constructs a new {@code EmbeddedProperty} instance with the specified 
	 * {@code Metamodel}, property type and property name.
	 * 
	 * @param metamodel The {@code Metamodel} instance which has this metamodel 
	 * property.
	 * @param type The property type.
	 * @param name The property name.
	 */
	public EmbeddedProperty(Metamodel<E> metamodel, Type type, String name) {
		super(metamodel, type, name);
	}
	
}
