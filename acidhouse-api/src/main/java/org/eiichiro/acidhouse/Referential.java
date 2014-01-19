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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code Referential} is a marker annotation to represent the annotated field 
 * is referential property (References the "Master" entity as read-only). Like 
 * this: 
 * <pre>
 * {@code @Entity}
 * public class Entity7 {
 * 
 * 	{@code @Key} String id;
 * 
 * 	int i;
 * 
 * 	{@code @Referential} Entity1 entity1;
 * 
 * }
 * 
 * ...
 * Entity1 entity1 = new Entity1();
 * entity1.id = "Reference1";
 * entity1.i = 1;
 * Entity7 entity71 = new Entity7();
 * entity71.id = "Key71";
 * entity71.i = 71;
 * entity71.entity1 = entity1;
 * Entity7 entity72 = new Entity7();
 * entity72.id = "Key72";
 * entity72.i = 72;
 * entity72.entity1 = entity1;
 * 
 * Session session = new AppEngineDatastoreSession();
 * Transaction transaction = session.beginTransaction();
 * 
 * try {
 * 	session.put(entity1);
 * 	session.put(entity71);
 * 	session.put(entity72);
 * 	transaction.commit();
 * } catch (Exception e) {
 * 	transaction.rollback();
 * 	throw e;
 * }
 * 
 * transaction = session.beginTransaction();
 * 
 * try {
 * 	entity1 = session.get(Entity1.class, "Reference1");
 * 	entity1.i = 11;		// from 1 to 11.
 * 	session.update(entity1);
 * 	transaction.commit();
 * } catch (Exception e) {
 * 	transaction.commit();
 * 	throw e;
 * } finally {
 * 	session.close();
 * }
 * 
 * session = new AppEngineDatastoreSession();
 * entity71 = session.get(Entity7.class, "Key71");
 * entity72 = session.get(Entity7.class, "Key72");
 * assertThat(entity71.entity1.i, is(11));
 * assertThat(entity72.entity1.i, is(11));
 * </pre>
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Referential {}
