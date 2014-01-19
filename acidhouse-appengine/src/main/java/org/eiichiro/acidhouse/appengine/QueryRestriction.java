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

import org.eiichiro.acidhouse.ComparableFilter;
import org.eiichiro.acidhouse.ComparableFilter.Operator;
import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.Order;

/**
 * {@code QueryRestriction} indicates whether the specified {@code Filter} and 
 * {@code Order} can be applied to Google App Engine datastore query or not.
 * If the specified {@code Filter} is restricted to Google App Engine datastore 
 * query, the {@code Filter} is processed as application-side filtering. So is 
 * {@code Order}.
 * See the restriction detail 
 * <a href="http://code.google.com/intl/en/appengine/docs/java/datastore/queriesandindexes.html#Restrictions_on_Queries">
 * here</a>.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class QueryRestriction {

	private Filter<?> filter;
	
	private Order<?, ?> order;
	
	private boolean restricted = false;
	
	/**
	 * Indicates whether the specified {@code Filter} is restricted to Google 
	 * App Engine datastore query or not.
	 * This method indicates the following cases as <b>restricted</b>: 
	 * <ul>
	 * <li>
	 * Property that the specified filter targets is not a root entity's 
	 * property.
	 * </li>
	 * <li>
	 * Operator type of the specified filter is inequality and the property that 
	 * the specified filter targets is not same as the previous filter's one.
	 * </li>
	 * </ul>
	 * 
	 * @param filter The filter to be indicated whether restricted or not.
	 * @return Whether the specified {@code Filter} is restricted to Google App 
	 * Engine datastore query or not.
	 */
	public boolean restricted(Filter<?> filter) {
		if (!filter.property().metamodel().isRoot()) {
			return true;
		}
		
		if (filter instanceof ComparableFilter<?>) {
			ComparableFilter<?> comparableFilter = (ComparableFilter<?>) filter;
			Operator operator = comparableFilter.operator();
			
			if (operator != Operator.EQUAL_TO) {
				if (this.filter != null && this.filter.property() != filter.property()) {
					return true;
				}
				
				this.filter = filter;
			}
		}
		
		return false;
	}
	
	/**
	 * Indicates whether the specified {@code Order} is restricted to Google App 
	 * Engine datastore query or not.
	 * This method indicates the following cases as <b>restricted</b>: 
	 * <ul>
	 * <li>
	 * Property that the specified order targets is not a root entity's property.
	 * </li>
	 * <li>
	 * If any inequality filter has been already specified, the property that 
	 * the filter targets is not same as the one that the first-qualified order 
	 * targets.
	 * </li>
	 * <li>
	 * All orders after the order indicated as restricted once.
	 * </li>
	 * </ul>
	 * 
	 * @param order The order to be indicated whether restricted or not.
	 * @return Whether the specified {@code Order} is restricted to Google App 
	 * Engine datastore query or not.
	 */
	public boolean restricted(Order<?, ?> order) {
		if (!order.property().metamodel().isRoot()) {
			restricted = true;
			return true;
		}
		
		if (restricted) {
			return true;
		}
		
		if (this.order == null) {
			if (filter != null && filter.property() != order.property()) {
				restricted = true;
				return true;
			}
			
			this.order = order;
		}
		
		return false;
	}
	
}
