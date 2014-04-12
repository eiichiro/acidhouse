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

/**
 * {@code Metamodel} represents the metamodel of Acid House entity.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public abstract class Metamodel<E> {

	private final Class<E> type;
	
	private final Metamodel<?> parent;
	
	private final String name;
	
	private boolean hasChild;
	
	/**
	 * Constructs a new {@code Metamodel} instance with the specified entity 
	 * type. This constructor is called when the represented entity is the root 
	 * entity (not owned child entity property).
	 * 
	 * @param type The entity type.
	 */
	public Metamodel(Class<E> type) {
		this(null, type, "");
	}
	
	/**
	 * Constructs a new {@code Metamodel} instance with the specified entity 
	 * type, parent metamodel and metamodel name. This constructor is called 
	 * if the represented entity is declared as owned child entity property.
	 * 
	 * @param parent The parent entity metamodel when this instance is declared 
	 * as owned child entity property.
	 * @param type The entity type.
	 * @param name The metamodel name when this instance is declared as owned 
	 * child entity property.
	 */
	public Metamodel(Metamodel<?> parent, Class<E> type, String name) {
		this.parent = parent;
		this.type = type;
		this.name = name;
		
		if (parent != null && !parent.hasChild) {
			parent.hasChild = true;
		}
	}

	/**
	 * Returns parent entity metamodel.
	 * This method returns <code>null</code>, when this instance is declared as 
	 * owned child entity property.
	 * 
	 * @return The parent entity metamodel.
	 */
	public Metamodel<?> parent() {
		return parent;
	}

	/**
	 * Returns entity metamodel name.
	 * This method returns empty string, when this instance is declared as root 
	 * entity.
	 * 
	 * @return The entity metamodel name.
	 */
	public String name() {
		return name;
	}
	
	/**
	 * Returns this entity metamodel instance represents root entity or not.
	 * 
	 * @return This entity metamodel instance represents root entity or not.
	 */
	public boolean isRoot() {
		return (parent() == null);
	}
	
	/**
	 * Returns <code>true</code> if this metamodel instance has a child 
	 * metamodel.
	 * 
	 * @return <code>true</code> if this metamodel instance has a child 
	 * metamodel.
	 */
	public boolean hasChild() {
		return hasChild;
	}
	
	/**
	 * Returns the entity type this metamodel represents.
	 * 
	 * @return The entity type this metamodel represents.
	 */
	public Class<E> type() {
		return type;
	}
	
	/**
	 * Returns the root metmaodel of this metmaodel instance.
	 * 
	 * @return The root metmaodel of this metmaodel instance.
	 */
	public Metamodel<?> root() {
		if (isRoot()) {
			return this;
		} else {
			return parent().root();
		}
	}
	
}
