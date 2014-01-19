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

import java.util.ArrayList;
import java.util.List;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

/**
 * {@code EntityValidator} validates the declaration of Acid House entity and 
 * returns the problems for metamodel generation.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class EntityValidator {

	private static final String KEY = "org.eiichiro.acidhouse.Key";
	
	private static final String JAVA_LANG_STRING = "java.lang.String";
	
	private static final String JAVA_LANG_LONG = "java.lang.Long";
	
	private static final String LONG = "long";
	
	private final Types types;
	
	/**
	 * Constructs a new {@code EntityValidator} with the specified type utility.
	 * 
	 * @param types Type utility.
	 */
	public EntityValidator(Types types) {
		this.types = types;
	}
	
	/**
	 * Validates the declaration of Acid House entity and returns the problems 
	 * for metamodel declaration.
	 * 
	 * @param type Type element of Entity.
	 * @return Problems for metamodel generation.
	 */
	public List<Problem> validate(TypeElement type) {
		List<Problem> problems = new ArrayList<Problem>();
		boolean declared = false;
		int constructor = 0;
		List<Element> keys = new ArrayList<Element>();
		
		for (Element element : type.getEnclosedElements()) {
			ElementKind kind = element.getKind();
			
			if (kind == ElementKind.CONSTRUCTOR) {
				constructor++;
				
				if (((ExecutableElement) element).getParameters().isEmpty()) {
					if (element.getModifiers().contains(Modifier.PUBLIC)) {
						declared = true;
					}
				}
				
			} else if (kind == ElementKind.FIELD) {
				for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
					Name name = ((TypeElement) mirror.getAnnotationType()
							.asElement()).getQualifiedName();
					
					if (name.contentEquals(KEY)) {
						keys.add(element);
						break;
					}
				}
			}
		}
		
		if (!declared && constructor > 0) {
			problems.add(new Problem(Kind.ERROR,
					"Entity class must have a public default constructor", type));
			return problems;
		}
		
		if (keys.isEmpty()) {
			problems.add(new Problem(Kind.ERROR,
					"Entity class must have one @Key property", type));
			return problems;
		} else if (keys.size() > 1) {
			for (Element key : keys) {
				problems.add(new Problem(Kind.ERROR,
						"@Key cannot be annotated on multiple properties", key));
			}
			
			return problems;
		}
		
		VariableElement variableElement = (VariableElement) keys.get(0);
		TypeMirror mirror = variableElement.asType();
		Element element = (mirror.getKind().isPrimitive()) 
				? types.boxedClass((PrimitiveType) mirror) : types.asElement(mirror);
		Name name = ((TypeElement) element).getQualifiedName();
		
		if (!name.contentEquals(JAVA_LANG_LONG)
				&& !name.contentEquals(JAVA_LANG_STRING)
				&& !name.contentEquals(LONG)) {
			problems.add(new Problem(Kind.ERROR, "@Key property must be ["
					+ JAVA_LANG_STRING + "] or [" + JAVA_LANG_LONG + "] or [" + LONG + "]", variableElement));
		}
		
		return problems;
	}
	
}
