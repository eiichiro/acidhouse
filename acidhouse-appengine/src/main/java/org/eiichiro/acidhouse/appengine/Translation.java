/*
 * Copyright (C) 2011-2012 Eiichiro Uchiumi. All Rights Reserved.
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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.ComparableFilter;
import org.eiichiro.acidhouse.Embedded;
import org.eiichiro.acidhouse.Entities;
import org.eiichiro.acidhouse.Key;
import org.eiichiro.acidhouse.Lock;
import org.eiichiro.acidhouse.Log;
import org.eiichiro.acidhouse.Log.State;
import org.eiichiro.acidhouse.Order;
import org.eiichiro.acidhouse.Referential;
import org.eiichiro.acidhouse.Transaction;
import org.eiichiro.acidhouse.Transient;
import org.eiichiro.reverb.lang.UncheckedException;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.storage.onestore.v3.OnestoreEntity;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;

/**
 * {@code Translation} provides translation utilities between Acid House data types 
 * and Google App Engine Datastore data types.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Translation {

	private static Logger logger = Logger.getLogger(Translation.class.getName());
	
	/** Google App Engine Datastore kind for {@code Lock}. */
	public static final String LOCK_KIND = "_" + Lock.class.getSimpleName();
	
	/** Google App Engine Datastore entity's property name for {@code Lock#transaction}. */
	public static final String TRANSACTION_PROPERTY = "transaction";
	
	/** Google App Engine Datastore entity's property name for {@code Lock#timestamp}. */
	public static final String TIMESTAMP_PROPERTY = "timestamp";
	
	/** Google App Engine Datastore kind for {@code Transaction}. */
	public static final String TRANSACTION_KIND = "_" + Transaction.class.getSimpleName();
	
	/** Google App Engine Datastore kind for {@code Log}. */
	public static final String LOG_KIND = "_" + Log.class.getSimpleName();
	
	/** Google App Engine Datastore entity's property name for {@code Log#operation}. */
	public static final String OPERATION_PROPERTY = "operation";
	
	/** Google App Engine Datastore entity's property name for {@code Log#entity}'s class. */
	public static final String CLASS_PROPERTY = "class";
	
	/** Google App Engine Datastore entity's property name for {@code Log#entity}'s Protocol Buffers message. */
	public static final String PROTO_PROPERTY = "proto";
	
	private Translation() {}
	
	/**
	 * Translates Google App Engine Datastore entities to Acid House entity with 
	 * {@code AppEngineDatastoreService}.
	 * 
	 * @param <E> The type of Acid House entity.
	 * @param clazz The {@code Class} of Acid House entity.
	 * @param entities Google App Engine Datastore entities.
	 * @param datastore {@code AppEngineDatastoreService}.
	 * @return Acid House entity translated from Google App Engine Datastore 
	 * entities.
	 */
	public static <E> E toObject(Class<E> clazz, List<Entity> entities, 
			Map<com.google.appengine.api.datastore.Key, Object> references, 
			AppEngineDatastoreService datastore) {
		return toObject(clazz, entities, references, datastore, 0);
	}
	
	@SuppressWarnings("unchecked")
	private static <E> E toObject(Class<E> clazz, List<Entity> entities, 
			Map<com.google.appengine.api.datastore.Key, Object> references, 
			AppEngineDatastoreService datastore, int hierarchy) {
		if (entities.size() == 0) {
			return null;
		}
		
		try {
			Iterator<Entity> iterator = entities.iterator();
			Entity entity = iterator.next();
			String kind = entity.getKind();
			
			if (kind.equals(LOCK_KIND) || kind.equals(TRANSACTION_KIND)) {
				return null;
			}
			
			E object = clazz.newInstance();
			List<Entity> group = new ArrayList<Entity>();
			
			while (iterator.hasNext()) {
				group.add(iterator.next());
			}
			
			for (Field field : clazz.getDeclaredFields()) {
				if (Modifier.isFinal(field.getModifiers())) {
					continue;
				}
				
				if (field.isAnnotationPresent(Transient.class)) {
					continue;
				}
				
				field.setAccessible(true);
				
				if (field.isAnnotationPresent(Key.class)) {
					String name = entity.getKey().getName();
					field.set(object, (name == null) ? entity.getKey().getId() : name);
					continue;
				}
				
				Object property = entity.getProperty(field.getName());
				Class<?> type = field.getType();
				
				if (field.isAnnotationPresent(Referential.class)) {
					if (property == null) {
						continue;
					}
					
					com.google.appengine.api.datastore.Key key 
							= (com.google.appengine.api.datastore.Key) property;
					
					if (!references.containsKey(key)) {
						// Optimization: Caching referential property.
						references.put(key, toObject(type, datastore.query(new Query(key)), references, datastore));
					}
					
					field.set(object, references.get(key));
					continue;
				}
				
				if (isCoreValue(type)) {
					if (property == null) {
						continue;
					}
					
					// int and java.lang.Integer fields are translated to 
					// int64Value (long) property in EntityProto, so I have to 
					// revert them to int...
					if ((type.getName().equals(CORE_VALUE_TYPES[16]) 
							|| type.getName().equals(CORE_VALUE_TYPES[20])) 
							&& (property.getClass().getName().equals(CORE_VALUE_TYPES[21]) 
									|| property.getClass().getName().equals(CORE_VALUE_TYPES[25]))) {
						long l = (Long) property;
						property = (int) l;
					}
					
					field.set(object, property);
				} else if (isCollection(type)) {
					if (property != null) {
						// Collection of core value type.
						field.set(object, property);
					} else {
						Class<?> collectionType = null;
						
						if (type.equals(List.class)) {
							collectionType = ArrayList.class;
						} else if (type.equals(Set.class)) {
							collectionType = HashSet.class;
						} else if (type.equals(SortedSet.class)) {
							collectionType = TreeSet.class;
						} else {
							collectionType = type;
						}
						
						Collection<Object> collection = (Collection<Object>) collectionType.newInstance();
						Class<?> element = (Class<?>) ((ParameterizedType) 
								field.getGenericType()).getActualTypeArguments()[0];
						List<List<Entity>> children = new ArrayList<List<Entity>>();
						List<Entity> child = null;
						com.google.appengine.api.datastore.Key previous = null;
						
						for (Entity e : group) {
							com.google.appengine.api.datastore.Key key = Keys.hierarchy(e.getKey()).get(hierarchy + 1);
							
							if (!key.getKind().equals(toKind(element))) {
								// Entities are assumed to be sorted by key.
								break;
							}
							
							if (!key.equals(previous)) {
								child = new ArrayList<Entity>();
								children.add(child);
							}
							
							child.add(e);
						}
						
						for (List<Entity> list : children) {
							collection.add(toObject(element, list, references, datastore, hierarchy + 1));
						}
						
						field.set(object, collection);
					}
					
				} else if (isArray(type)) {
					logger.warning("Array type translation is not supported: field [" 
							+ field + "]; Use Collection type instead");
				} else if (isEntity(type)) {
					List<Entity> child = new ArrayList<Entity>();
					
					for (Entity e : group) {
						com.google.appengine.api.datastore.Key key = Keys.hierarchy(e.getKey()).get(hierarchy + 1);
						
						if (!key.getKind().equals(toKind(type))) {
							// Entities are assumed to be sorted by key.
							break;
						}
						
						child.add(e);
					}
					
					field.set(object, toObject(type, child, references, datastore, hierarchy + 1));
				} else if (isEmbedded(type)) {
					Object embedded = type.newInstance();
					
					for (Field f : type.getDeclaredFields()) {
						if (Modifier.isFinal(f.getModifiers())) {
							continue;
						}
						
						f.setAccessible(true);
						Object p = entity.getProperty(field.getName() + "." + f.getName());
						Class<?> t = f.getType();
						
						if (isCoreValue(t)) {
							if (p == null) {
								continue;
							}
							
							// int and java.lang.Integer fields are translated to 
							// int64Value (long) property in EntityProto, so I have to 
							// revert them to int...
							if ((t.getName().equals(CORE_VALUE_TYPES[16]) 
									|| t.getName().equals(CORE_VALUE_TYPES[20])) 
									&& (p.getClass().getName().equals(CORE_VALUE_TYPES[21]) 
											|| p.getClass().getName().equals(CORE_VALUE_TYPES[25]))) {
								long l = (Long) p;
								p = (int) l;
							}
							
							f.set(embedded, p);
						} else if (isCollection(t)) {
							if (p != null) {
								// Collection of core value type.
								// Because types other than core value type / collection of core value type 
								// are not persisted in an embedded type.
								f.set(embedded, p);
							} else {
								Class<?> collectionType = null;
								
								if (t.equals(List.class)) {
									collectionType = ArrayList.class;
								} else if (t.equals(Set.class)) {
									collectionType = HashSet.class;
								} else if (t.equals(SortedSet.class)) {
									collectionType = TreeSet.class;
								} else {
									collectionType = type;
								}
								
								f.set(embedded, collectionType.newInstance());
							}
							
						} else {
							logger.warning("Type translation other than core value type / collection of core value type "
									+ "is not supported in an embedded type: field [" + f + "]");
						}
					}
					
					field.set(object, embedded);
				} else {
					logger.warning("User defined type translation is not supported: field [" 
							+ field + "]");
				}
			}
			
			return object;
		} catch (Exception e) {
			throw new UncheckedException(e);
		}
	}
	
	/**
	 * Translates Acid House entity to Google App Engine Datastore entities.
	 * 
	 * @param entity Acid House entity.
	 * @return Google App Engine Datastore entities translated from Acid House 
	 * entity.
	 */
	public static List<Entity> toEntities(Object entity) {
		return toEntities(null, entity);
	}
	
	/** Types available for Google App Engine Datastore. */
	public static String[] CORE_VALUE_TYPES = {
		"boolean", 
		"com.google.appengine.api.datastore.Blob", 
		"com.google.appengine.api.datastore.Category", 
		"com.google.appengine.api.datastore.Email", 
		"com.google.appengine.api.datastore.GeoPt", 
		"com.google.appengine.api.datastore.IMHandle", 
		"com.google.appengine.api.datastore.Key", 
		"com.google.appengine.api.datastore.Link", 
		"com.google.appengine.api.datastore.PhoneNumber", 
		"com.google.appengine.api.datastore.PostalAddress", 
		"com.google.appengine.api.datastore.Rating", 
		"com.google.appengine.api.datastore.ShortBlob", 
		"com.google.appengine.api.datastore.Text", 
		"com.google.appengine.api.users.User", 
		"double", 
		"float", 
		"int", 
		"java.lang.Boolean", 
		"java.lang.Double", 
		"java.lang.Float", 
		"java.lang.Integer", 
		"java.lang.Long", 
		"java.lang.Short", 
		"java.lang.String", 
		"java.util.Date", 
		"long", 
		"short"
	};
	
	/** Collection types available for Google App Engine Datastore. */
	public static String[] CORE_COLLECTION_TYPES = {
		"java.util.ArrayList", 
		"java.util.HashSet", 
		"java.util.LinkedHashSet", 
		"java.util.LinkedList", 
		"java.util.List", 
		"java.util.Set", 
		"java.util.SortedSet", 
		"java.util.Stack", 
		"java.util.TreeSet", 
		"java.util.Vector"
	};
	
	/** Prefix of array type. */
	public static String ARRAY_TYPE = "[";
	
	/**
	 * Translates Acid House entity to Google App Engine Datastore entities with 
	 * the specified {@code Key} of parent.
	 * 
	 * @param parent The {@code Key} of parent.
	 * @param entity Acid House entity.
	 * @return Google App Engine Datastore entities translated from Acid House 
	 * entity.
	 */
	public static List<Entity> toEntities(com.google.appengine.api.datastore.Key parent, Object entity) {
		List<Entity> entities = new ArrayList<Entity>();
		Class<?> clazz = entity.getClass();
		
		if (!clazz.isAnnotationPresent(org.eiichiro.acidhouse.Entity.class)) {
			throw new IllegalArgumentException("Entity [" + clazz
					+ "] must be annotated with @org.eiichiro.acidhouse.Entity");
		}
		
		Entity e = new Entity(Keys.create(parent, toKind(clazz), Entities.keyValue(entity)));
		entities.add(e);
		
		for (Field field : clazz.getDeclaredFields()) {
			if (field.isAnnotationPresent(Key.class)) {
				continue;
			}
			
			if (field.isAnnotationPresent(Transient.class)) {
				continue;
			}
			
			try {
				field.setAccessible(true);
				Object object = field.get(entity);
				
				if (object == null) {
					continue;
				}
				
				Class<?> type = field.getType();
				boolean unindexed = field.isAnnotationPresent(Unindexed.class);
				
				if (field.isAnnotationPresent(Referential.class)) {
					com.google.appengine.api.datastore.Key reference 
							= Keys.create(toKind(type), Entities.keyValue(object));
					setProperty(e, field.getName(), reference, unindexed);
					continue;
				}
				
				if (isCoreValue(type)) {
					setProperty(e, field.getName(), object, unindexed);
				} else if (isCollection(type)) {
					Collection<?> collection = (Collection<?>) object;
					Class<?> element = (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
					
					if (isCoreValue(element)) {
						setProperty(e, field.getName(), collection, unindexed);
					} else if (isEntity(element)) {
						for (Object o : collection) {
							entities.addAll(toEntities(e.getKey(), o));
						}
						
					} else {
						logger.warning("Collection of non-core value type / non-entity type translation "
								+ "is not supported: field [" + field + "]");
					}
					
				} else if (isArray(type)) {
					logger.warning("Array type translation is not supported: field [" 
							+ field + "]; Use Collection type instead");
				} else if (isEntity(type)) {
					entities.addAll(toEntities(e.getKey(), object));
				} else if (isEmbedded(type)) {
					for (Field f : type.getDeclaredFields()) {
						if (f.isAnnotationPresent(Transient.class)) {
							continue;
						}
						
						f.setAccessible(true);
						Object o = f.get(object);
						
						if (o == null) {
							continue;
						}
						
						Class<?> t = f.getType();
						
						if (isCoreValue(t) || (isCollection(t) && isCoreValue(
								(Class<?>) ((ParameterizedType) f.getGenericType()).getActualTypeArguments()[0]))) {
							setProperty(e, field.getName() + "." + f.getName(), 
									o, f.isAnnotationPresent(Unindexed.class));
						} else {
							logger.warning("Type translation other than core value type / collection of core value type "
									+ "is not supported in an embedded type: field [" + f + "]");
						}
					}
					
				} else {
					logger.warning("User defined type translation is not supported: field [" 
							+ field + "]");
				}

			} catch (Exception exception) {
				throw new UncheckedException(exception);
			}
		}
		
		return entities;
	}
	
	private static void setProperty(Entity entity, String name, Object value, boolean unindexed) {
		if (unindexed) {
			entity.setUnindexedProperty(name, value);
		} else {
			entity.setProperty(name, value);
		}
	}
	
	/**
	 * Translates Google App Engine Datastore entity to Acid House {@code Lock} 
	 * entity.
	 * 
	 * @param entity oogle App Engine Datastore entity.
	 * @return Acid House {@code Lock} entity.
	 */
	public static Lock toLock(Entity entity) {
		return new Lock(entity.getKey().getName(), 
				(String) entity.getProperty(TRANSACTION_PROPERTY), 
				(Date) entity.getProperty(TIMESTAMP_PROPERTY));
	}
	
	/**
	 * Translates Acid House {@code Lock} entity to Google App Engine Datastore 
	 * entity with the specified {@code Key} of parent.
	 * 
	 * @param lock Acid House {@code Lock} entity.
	 * @param parent The {@code Key} of parent.
	 * @return Google App Engine Datastore entity translated from Acid House 
	 * {@code Lock} entity.
	 */
	public static Entity toEntity(Lock lock, com.google.appengine.api.datastore.Key parent) {
		Entity entity = new Entity(LOCK_KIND, lock.id(), parent);
		entity.setUnindexedProperty(TRANSACTION_PROPERTY, lock.transaction());
		entity.setUnindexedProperty(TIMESTAMP_PROPERTY, lock.timestamp());
		return entity;
	}
	
	/**
	 * Translates Google App Engine Datastore entities to Acid House 
	 * {@code AppEngineTransaction} entity with {@code AppEngineDatastoreService}.
	 * 
	 * @param transactions Google App Engine Datastore entities.
	 * @param datastore {@code AppEngineDatastoreService}.
	 * @return Acid House {@code AppEngineTransaction} entity translated from 
	 * Google App Engine Datastore entities.
	 */
	@SuppressWarnings("unchecked")
	public static AppEngineGlobalTransaction toTransaction(
			Iterable<Entity> transactions, AppEngineDatastoreService datastore) {
		AppEngineGlobalTransaction transaction = null;
		Map<Long, Entity> logs = new TreeMap<Long, Entity>();
		
		for (Entity tx : transactions) {
			if (tx.getKind().equals(TRANSACTION_KIND)) {
				transaction = new AppEngineGlobalTransaction(tx.getKey().getName(), null, null);
			} else if (tx.getKind().equals(LOG_KIND)) {
				logs.put(tx.getKey().getId(), tx);
			}
		}
		
		for (Long sequence : logs.keySet()) {
			Entity log = logs.get(sequence);
			List<Entity> entities = new ArrayList<Entity>();
			
			for (Blob proto : (List<Blob>) log.getProperty(PROTO_PROPERTY)) {
				OnestoreEntity.EntityProto entityProto = new OnestoreEntity.EntityProto();
				entityProto.mergeFrom(proto.getBytes());
				entities.add(EntityTranslator.createFromPb(entityProto));
			}
			
			try {
				Log l = new Log(sequence, 
						Enum.valueOf(Log.Operation.class, (String) log.getProperty(OPERATION_PROPERTY)), 
						toObject(Class.forName((String) log.getProperty(CLASS_PROPERTY)), 
								entities, new HashMap<com.google.appengine.api.datastore.Key, Object>(0), datastore));
				l.state(State.PREPARED);
				transaction.logs().add(l);
			} catch (ClassNotFoundException e) {
				throw new UncheckedException(e);
			}
		}
		
		return transaction;
	}
	
	/**
	 * Translates Acid House {@code AppEngineTransaction} entity to Google App 
	 * Engine Datastore entities with the specified {@code Key} of parent.
	 * 
	 * @param transaction Acid House {@code Lock} entity.
	 * @param parent The {@code Key} of parent.
	 * @return Google App Engine Datastore entities translated from Acid House 
	 * {@code AppEngineTransaction} entity.
	 */
	public static List<Entity> toEntities(AppEngineTransaction transaction,
			com.google.appengine.api.datastore.Key parent) {
		List<Entity> entities = new ArrayList<Entity>();
		com.google.appengine.api.datastore.Key key 
				= Keys.create(parent, TRANSACTION_KIND, transaction.id());
		entities.add(new Entity(key));
		
		for (Log log : transaction.logs()) {
			Entity entity = new Entity(Keys.create(key, LOG_KIND, log.sequence()));
			entity.setUnindexedProperty(OPERATION_PROPERTY, log.operation().name());
			entity.setUnindexedProperty(CLASS_PROPERTY, log.entity().getClass().getName());
			List<Blob> blobs = new ArrayList<Blob>();
			entity.setUnindexedProperty(PROTO_PROPERTY, blobs);
			entities.add(entity);
			
			for (Entity e : toEntities(log.entity())) {
				EntityProto proto = EntityTranslator.convertToPb(e);
				byte[] bytes = proto.toByteArray();
				blobs.add(new Blob(bytes));
			}
		}
		
		return entities;
	}
	
	/**
	 * Translates {@code Class} to Google App Engine Datastore entity's kind 
	 * name.
	 * 
	 * @param clazz The {@code Class} of Acid House entity.
	 * @return The kind name translated from Acid House entity class.
	 */
	public static String toKind(Class<?> clazz) {
		if (clazz.getAnnotation(org.eiichiro.acidhouse.Entity.class) == null) {
			throw new IllegalArgumentException("Entity class [" + clazz
					+ "] must be annotated by @org.eiichiro.acidhouse.Entity");
		}
		
		return clazz.getSimpleName();
	}
	
	/**
	 * Translates {@code ComparableFilter.Operator} to Google App Engine 
	 * Datastore {@code FilterOperator}.
	 * 
	 * @param operator Acid House {@code ComparableFilter.Operator}.
	 * @return Google App Engine Datastore {@code FilterOperator} translated 
	 * from Acid House {@code ComparableFilter.Operator}.
	 */
	public static FilterOperator toFilterOperator(ComparableFilter.Operator operator) {
		if (operator == ComparableFilter.Operator.EQUAL_TO) {
			return FilterOperator.EQUAL;
		} else if (operator == ComparableFilter.Operator.GREATER_THAN) {
			return FilterOperator.GREATER_THAN;
		} else if (operator == ComparableFilter.Operator.GREATER_THAN_OR_EQUAL_TO) {
			return FilterOperator.GREATER_THAN_OR_EQUAL;
		} else if (operator == ComparableFilter.Operator.LESS_THAN) {
			return FilterOperator.LESS_THAN;
		} else if (operator == ComparableFilter.Operator.LESS_THAN_OR_EQUAL_TO) {
			return FilterOperator.LESS_THAN_OR_EQUAL;
		} else if (operator == ComparableFilter.Operator.NOT_EQUAL_TO) {
			return FilterOperator.NOT_EQUAL;
		} else {
			throw new UnsupportedOperationException("Operator [" + operator
					+ "] is not supported by Google App Engine Datastore");
		}
	}
	
	/**
	 * Translates {@code Order.Direction} to Google App Engine Datastore 
	 * {@code SortDirection}.
	 * 
	 * @param direction Acid House {@code Order.Direction}.
	 * @return Google App Engine Datastore {@code SortDirection} translated 
	 * from Acid House {@code Order.Direction}.
	 */
	public static SortDirection toSortDirection(Order.Direction direction) {
		if (direction == Order.Direction.ASC) {
			return SortDirection.ASCENDING;
		} else if (direction == Order.Direction.DESC) {
			return SortDirection.DESCENDING;
		} else {
			throw new UnsupportedOperationException("Direction [" + direction
					+ "] is not supported by Google App Engine Datastore");
		}
	}
	
	private static boolean isEntity(Class<?> type) {
		return type.isAnnotationPresent(org.eiichiro.acidhouse.Entity.class);
	}
	
	private static boolean isEmbedded(Class<?> type) {
		return type.isAnnotationPresent(Embedded.class);
	}
	
	private static boolean isCollection(Class<?> type) {
		return Arrays.binarySearch(CORE_COLLECTION_TYPES, type.getName()) >= 0;
	}
	
	private static boolean isArray(Class<?> type) {
		return type.getName().startsWith(ARRAY_TYPE);
	}
	
	private static boolean isCoreValue(Class<?> type) {
		return Arrays.binarySearch(CORE_VALUE_TYPES, type.getName()) >= 0;
	}
	
}
