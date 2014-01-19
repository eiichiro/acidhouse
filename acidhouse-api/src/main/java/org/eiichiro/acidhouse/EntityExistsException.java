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
 * {@code EntityExistsException} is thrown by {@code Session#put()} 
 * operation when the specified entity has already existed in the datastore.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class EntityExistsException extends RuntimeException {

	private static final long serialVersionUID = -5443340977274792315L;

	public EntityExistsException(Object key) {
		super("Entity corresponding to the key [" + key + "] has already existed");
	}
	
}
