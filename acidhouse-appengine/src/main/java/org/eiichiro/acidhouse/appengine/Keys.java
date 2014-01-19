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
package org.eiichiro.acidhouse.appengine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

/**
 * {@code Keys} are utility methods for Google App Engine Datastore {@code Key}-
 * related operations.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Keys {

	private Keys() {}
	
	/**
	 * Creates {@code Key} with the specified kind name and id.
	 * 
	 * @param kind The kind name of the specified entity.
	 * @param key The key of the entity.
	 * @return {@code Key} created with the specified kind name and key.
	 */
	public static Key create(String kind, Object key) {
		return create(null, kind, key);
	}
	
	/**
	 * Creates {@code Key} with the specified parent, kind name and key.
	 * 
	 * @param parent The {@code Key} of the parent.
	 * @param kind The kind name of the entity.
	 * @param key The key of the entity.
	 * @return {@code Key} created with the specified parent, kind name and key.
	 */
	public static Key create(Key parent, String kind, Object key) {
		if (key instanceof String) {
			return (parent == null) ? KeyFactory.createKey(kind, (String) key) : KeyFactory.createKey(parent, kind, (String) key);
		} else if (key instanceof Long) {
			return (parent == null) ? KeyFactory.createKey(kind, (Long) key) : KeyFactory.createKey(parent, kind, (Long) key);
		} else if (key instanceof Key) {
			return (Key) key;
		} else {
			throw new IllegalArgumentException("'key' must be an instance of ["
					+ String.class.getName() + "] or [" + Long.class.getName()
					+ "] or [" + Key.class.getName() + "]");
		}
	}
	
	/**
	 * Gets ancestor {@code Key} from the specified {@code Key}.
	 * 
	 * @param key The {@code Key} of the specified entity.
	 * @return Ancestor {@code Key} of the specified {@code Key}.
	 */
	public static Key ancestor(Key key) {
		Key ancestor = key;
		
		while (ancestor.getParent() != null) {
			ancestor = ancestor.getParent();
		}
		
		return ancestor;
	}
	
	/**
	 * Gets the {@code Key} hierarchy of the specified entity.
	 * The ancestor is the first entry.
	 * 
	 * @param key The {@code Key} of the specified entity.
	 * @return The {@code Key} hierarchy of the specified entity.
	 */
	public static List<Key> hierarchy(Key key) {
		List<Key> hierarchy = new ArrayList<Key>();
		hierarchy.add(key);
		
		while (key.getParent() != null) {
			key = key.getParent();
			hierarchy.add(key);
		}
		
		Collections.reverse(hierarchy);
		return hierarchy;
	}
	
}
