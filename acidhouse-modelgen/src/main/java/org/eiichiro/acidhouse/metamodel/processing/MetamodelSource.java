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

import java.io.File;
import java.io.PrintWriter;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Generated;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

/**
 * {@code MetamodelSource} represents an Acid House entity metamodel Java 
 * source file.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class MetamodelSource {

	private static final String PACKAGE_TEMPLATE = "package %s;\n\n";

	private static final String IMPORT_TEMPLATE = "import "
			+ Generated.class.getName() + ";\n"
			+ "import org.eiichiro.acidhouse.metamodel.Metamodel;\n\n";

	private static final String CLASS_TEMPLATE = "@Generated(\""
			+ MetamodelGenerator.class.getName() + "\")\n"
			+ "public class %s extends Metamodel<%s> {\n\n" + "%s" + "}\n";

	private static final String CONSTRUCTOR_TEMPLATE = "\tpublic %s() {\n"
			+ "\t\tsuper(%s.class);\n" + "\t}\n\n"
			+ "\tpublic %s(Metamodel<?> parent, String name) {\n"
			+ "\t\tsuper(parent, %s.class, name);\n" + "\t}\n\n";

	private static final String COMPARABLE_PROPERTY_TEMPLATE 
			= "\tpublic org.eiichiro.acidhouse.metamodel.ComparableProperty<%s, %s> %s"
			+ " = new org.eiichiro.acidhouse.metamodel.ComparableProperty<%s, %s>(this, %s.class, \"%s\");\n\n";
	
	private static final String NON_COMPARABLE_PROPERTY_TEMPLATE 
			= "\tpublic org.eiichiro.acidhouse.metamodel.Property<%s, %s> %s"
			+ " = new org.eiichiro.acidhouse.metamodel.Property<%s, %s>(" 
			+ "this, %s.class, \"%s\");\n\n";

	private static final String METAMODEL_TEMPLATE 
			= "\tpublic %s %s = new %s(this, \"%s\");\n\n";
	
	private static final String EMBEDDED_CLASS_TEMPLATE = "\t@Generated(\"" 
			+ MetamodelGenerator.class.getName() + "\")\n" 
			+ "\tpublic static class %s extends org.eiichiro.acidhouse.metamodel.EmbeddedProperty<%s, %s> {\n\n" 
			+ "%s" + "\t}\n\n";
	
	private static final String EMBEDDED_CONSTRUCTOR_TEMPLATE 
			= "\t\tprivate %s(Metamodel<%s> parent, String name) {\n" 
			+ "\t\t\tsuper(parent, %s.class, name);\n" + "\t\t}\n\n";
	
	private static final String EMBEDDED_TEMPLATE 
			= "\tpublic %s %s = new %s(this, \"%s\");\n\n";

	private final TypeElement element;

	private final ProcessingEnvironment environment;

	private final String packageName;

	private final String entityName;

	private final String metamodelName;

	/**
	 * Constructs a new {@code MetamodelSource} instance with the specified 
	 * entity class representation and processing environment.
	 * 
	 * @param element Acid House entity class representation.
	 * @param environment Processing environment.
	 */
	public MetamodelSource(TypeElement element, ProcessingEnvironment environment) {
		this.element = element;
		this.environment = environment;
		Elements elementUtils = environment.getElementUtils();
		packageName = elementUtils.getPackageOf(element).getQualifiedName().toString();
		entityName = element.getSimpleName().toString();
		metamodelName = toMetamodelName(entityName);
	}

	/**
	 * Save this instance representation as a Java source file. The Java source 
	 * file is saved as the name adding '_' prefix to the entity class name and 
	 * into the same package as the entity class. If the source file has been 
	 * already saved, this method deletes the file and then creates the file 
	 * newly.
	 * 
	 * @throws Exception If source file saving is failed due to any exception.
	 */
	public void save() throws Exception {
		Filer filer = environment.getFiler();
		FileObject resource = filer.getResource(StandardLocation.SOURCE_OUTPUT,
				packageName, metamodelName + ".java");
		File file = new File(resource.toUri().toString());

		if (file.exists()) {
			file.delete();
		}
		
		StringBuilder source = new StringBuilder();
		source.append(String.format(PACKAGE_TEMPLATE, packageName).toString());
		source.append(IMPORT_TEMPLATE);
		StringBuilder buffer = new StringBuilder(String.format(
				CONSTRUCTOR_TEMPLATE, metamodelName, entityName, metamodelName,
				entityName).toString());
		StringBuilder embedded = new StringBuilder();
		Set<String> embeddeds = new HashSet<String>();
		Elements elementUtils = environment.getElementUtils();
		
		for (VariableElement field : ElementFilter.fieldsIn(
				elementUtils.getAllMembers(element))) {
			if (!isTransient(field)) {
				String name = field.getSimpleName().toString();
				TypeMirror type = field.asType();
				
				if (isEmbedded(type)) {
					if (!embeddeds.contains(type.toString())) {
						embeddeds.add(type.toString());
						embedded.append(toEmbedded(type));
					}
					
					String e = metamodelName + "." + toEmbeddedName(environment.getTypeUtils().asElement(field.asType()).getSimpleName().toString());
					buffer.append(String.format(EMBEDDED_TEMPLATE, e, name, e, name));
				} else if (isEntity(type)) {
					// Owned child entity object.
					String metamodel = toMetamodelName(field.asType()
							.toString());
					buffer.append(String.format(METAMODEL_TEMPLATE,
							metamodel, name, metamodel, name));
				} else if (isComparable(type)) {
					buffer.append(new Formatter().format(
							COMPARABLE_PROPERTY_TEMPLATE, entityName,
							box(type), name, entityName, box(type), toRawType(box(type)),
							name).toString());
				} else {
					buffer.append(new Formatter().format(
							NON_COMPARABLE_PROPERTY_TEMPLATE, entityName,
							box(type), name, entityName, box(type), toRawType(box(type)),
							name).toString());
				}
			}
		}
		
		source.append(String.format(CLASS_TEMPLATE, metamodelName, entityName, buffer.append(embedded)));
		PrintWriter writer = new PrintWriter(filer.createSourceFile(
				packageName + "." + metamodelName, element).openWriter());
		writer.write(source.toString());
		writer.close();
	}

	String toMetamodelName(String type) {
		// To be configurable?
		int i = type.lastIndexOf(".");
		
		if (i < 0) {
			return "_" + type;
		} else {
			return type.substring(0, type.lastIndexOf(".") + 1) + "_" + type.substring(type.lastIndexOf(".") + 1);
		}
	}

	private String toEmbeddedName(String type) {
		// To be configurable?
		return "_" + type;
	}
	
	private String toEmbedded(TypeMirror type) throws Exception {
		Element element = environment.getTypeUtils().asElement(type);
		StringBuilder builder = new StringBuilder(String.format(EMBEDDED_CONSTRUCTOR_TEMPLATE, 
				toEmbeddedName(element.getSimpleName().toString()),
				entityName, type.toString()).toString());
		
		for (VariableElement field : ElementFilter.fieldsIn(
				environment.getElementUtils().getAllMembers((TypeElement) element))) {
			if (!isTransient(field)) {
				String name = field.getSimpleName().toString();
				TypeMirror t = field.asType();
				
				if (isComparable(t)) {
					builder.append(String.format("\t" + COMPARABLE_PROPERTY_TEMPLATE, 
							entityName, box(t), name, entityName, box(t), toRawType(box(t)), name).toString());
				} else {
					builder.append(String.format("\t" + NON_COMPARABLE_PROPERTY_TEMPLATE, 
							entityName, box(t), name, entityName, box(t), toRawType(box(t)), name).toString());
				}
			}
		}
		
		String embedded = String.format(EMBEDDED_CLASS_TEMPLATE, 
				toEmbeddedName(element.getSimpleName().toString()),
				entityName, type.toString(), builder.toString()).toString();
		return embedded;
	}
	
	private boolean isTransient(VariableElement field) {
		for (AnnotationMirror annotationMirror : field.getAnnotationMirrors()) {
			if (annotationMirror.getAnnotationType().toString().equals(
					"org.eiichiro.acidhouse.Transient")) {
				return true;
			}
		}

		return false;
	}

	private boolean isEmbedded(TypeMirror type) {
		Element element = environment.getTypeUtils().asElement(type);
		
		if (element == null) {
			return false;
		}
		
		for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
			if (mirror.getAnnotationType().toString().equals("org.eiichiro.acidhouse.Embedded")) {
				return true;
			}
		}
		
		return false;
	}

	private boolean isEntity(TypeMirror type) {
		Element element = environment.getTypeUtils().asElement(type);
		
		if (element == null) {
			return false;
		}
		
		for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
			if (mirror.getAnnotationType().toString().equals("org.eiichiro.acidhouse.Entity")) {
				return true;
			}
		}
		
		return false;
	}
	
	private boolean isComparable(TypeMirror type) throws Exception {
		if (type.getKind().isPrimitive()) {
			return true;
		}
		
		Element element = environment.getTypeUtils().asElement(type);
		
		if (element == null) {
			return false;
		}
		
		for (TypeMirror mirror : ((TypeElement) element).getInterfaces()) {
			DeclaredType declaredType = (DeclaredType) mirror;
			
			if (((TypeElement) declaredType.asElement()).getQualifiedName()
					.toString().equals("java.lang.Comparable")) {
				return true;
			} else if (isComparable(mirror)) {
				return true;
			}
		}
		
		return false;
	}
	
	private TypeMirror box(TypeMirror type) {
		if (type.getKind().isPrimitive()) {
			return environment.getTypeUtils().boxedClass((PrimitiveType) type).asType();
		}

		return type;
	}
	
	private String toRawType(TypeMirror type) {
		Element element = environment.getTypeUtils().asElement(type);
		return (element == null) ? type.toString() : ((TypeElement) environment
				.getTypeUtils().asElement(type)).getQualifiedName().toString();
	}
	
}
