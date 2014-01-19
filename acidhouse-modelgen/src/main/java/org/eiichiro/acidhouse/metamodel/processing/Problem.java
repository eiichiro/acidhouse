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
package org.eiichiro.acidhouse.metamodel.processing;

import javax.lang.model.element.Element;
import javax.tools.Diagnostic.Kind;

/**
 * {@code Problem} represents a problem occurred in the metamodel generation.
 * This problem is reported by {@code javax.annotation.processing.Messager}.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Problem {

	private final Kind kind;
	
	private final String message;
	
	private final Element element;

	/**
	 * Constructs a new {@code Problem} with the specified problem kind, message 
	 * and element.
	 * 
	 * @param kind The kind of this problem.
	 * @param message The message to be reported.
	 * @param element The element this problem occurred in.
	 */
	public Problem(Kind kind, String message, Element element) {
		this.kind = kind;
		this.message = message;
		this.element = element;
	}

	/**
	 * Returns the kind of this problem.
	 * 
	 * @return The kind of this problem.
	 */
	public Kind kind() {
		return kind;
	}

	/**
	 * Returns the message to be reported.
	 * 
	 * @return The message to be reported.
	 */
	public String message() {
		return message;
	}

	/**
	 * Returns the element this problem occurred in.
	 * 
	 * @return The element this problem occurred in.
	 */
	public Element element() {
		return element;
	}
	
}
