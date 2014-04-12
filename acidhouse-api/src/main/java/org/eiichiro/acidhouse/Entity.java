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
 * {@code @Entity} is a marker annotation to tell Acid House that the annotated 
 * class is an entity class to be managed by Acid House.
 * {@code Entity} is a POJO (Plain Old Java Object), and can represent a 
 * hierarchical structure (not relationship) as the ownership of member objects. 
 * The hierarchy is appropriately mapped to the datastore-specific data model by 
 * {@code ResourceManager} implementation, like this: 
 * <pre>
 * {@code @Entity}
 * public class Entity3 {
 * 
 * 	{@code @Key} String id;
 * 
 * 	int i;
 * 
 * 	Entity1 entity1;
 * 
 * 	List&lt;Entity2&gt; entity2s;
 * 
 * }
 * 
 * {@code @Entity}
 * public class Entity1 {
 * 
 * 	{@code @Key} String id;
 * 
 * 	int i;
 * 
 * }
 * 
 * {@code @Entity}
 * public class Entity2 {
 * 
 * 	{@code @Key} String id;
 * 
 * 	List&lt;Integer&gt; integers;
 * 
 * }
 * </pre>
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.TYPE)
public @interface Entity {}
