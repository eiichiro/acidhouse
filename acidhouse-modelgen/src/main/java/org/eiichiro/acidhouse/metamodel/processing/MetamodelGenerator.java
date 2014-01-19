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

import java.util.List;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

/**
 * {@code MetamodelGenerator} is a "JSR 269 annotation processor" based Java 
 * source code generator of Acid House entity metamodel.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
@SupportedSourceVersion(SourceVersion.RELEASE_6)
@SupportedAnnotationTypes("org.eiichiro.acidhouse.Entity")
public class MetamodelGenerator extends AbstractProcessor {

	/**
	 * Generates metamodel Java source file of the specified Acid House entity 
	 * class representations.
	 * 
	 * @param elements Acid House entity class representations.
	 * @param environment Processing environment.
	 */
	@Override
	public boolean process(Set<? extends TypeElement> elements,
			RoundEnvironment environment) {
		Messager messager = processingEnv.getMessager();
		
		for (TypeElement element : elements) {
			Set<? extends Element> elementsAnnotatedWith = environment.getElementsAnnotatedWith(element);
			
			for (Element elementAnnotatedWith : elementsAnnotatedWith) {
				TypeElement typeElement = (TypeElement) elementAnnotatedWith;
				EntityValidator validator = new EntityValidator(processingEnv.getTypeUtils());
				List<Problem> problems = validator.validate(typeElement);
				
//				for (Problem problem : problems) {
//					System.out.println(problem);
//				}
				
				boolean error = false;
				
				for (Problem problem : problems) {
					if (problem.kind() == Kind.ERROR) {
						error = true;
					}
					
					messager.printMessage(problem.kind(), problem.message(), problem.element());
				}
				
				if (!error) {
					MetamodelSource source = new MetamodelSource(typeElement, processingEnv);
					
					try {
						source.save();
					} catch (Exception e) {
						messager.printMessage(Kind.ERROR, "Metamodel ["
								+ source.toMetamodelName(typeElement.getQualifiedName().toString())
								+ "] cannot be saved due to [" + e + "]",
								typeElement);
						e.printStackTrace();
					}
				}
			}
		}
		
		return true;
	}

}
