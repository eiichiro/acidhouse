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
 * {@code Reference} is a built-in entity that represents an entity reference 
 * (referential property).
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Reference {

	private final String id;
	
	/**
	 * Constructs a new {@code Reference} with the specified entity id being 
	 * referenced.
	 * 
	 * @param id The entity id being referenced.
	 */
	public Reference(String id) {
		this.id = id;
	}

	/**
	 * Gets the entity id being referenced.
	 * 
	 * @return The entity id being referenced.
	 */
	public String id() {
		return id;
	}
	
}
