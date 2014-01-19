package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.metamodel.Metamodel;

public class _Entity10 extends Metamodel<Entity10> {

	public _Entity10() {
		super(Entity10.class);
	}

	public _Entity10(Metamodel<?> parent, String name) {
		super(parent, Entity10.class, name);
	}
	
	public org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Long> key = new org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Long>(this, java.lang.Long.class, "key");

	public org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer> i = new org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer>(this, java.lang.Integer.class, "i");

	public _Entity10.Embedded1$ embedded1 = new _Entity10.Embedded1$(this, "embedded1");

	public _Entity10.Embedded1$ embedded12 = new _Entity10.Embedded1$(this, "embedded12");

	public _Entity10.Embedded2$ embedded2 = new _Entity10.Embedded2$(this, "embedded2");

	public static class Embedded1$ extends org.eiichiro.acidhouse.metamodel.EmbeddedProperty<Entity10, Embedded1> {

		private Embedded1$(Metamodel<Entity10> parent, String name) {
			super(parent, Embedded1.class, name);
		}

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, long[]> ls = new org.eiichiro.acidhouse.metamodel.Property<Entity10, long[]>(this, long[].class, "ls");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<java.lang.Integer>> integers = new org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<java.lang.Integer>>(this, java.util.List.class, "integers");

		public org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer> i = new org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer>(this, java.lang.Integer.class, "i");
		
		public org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer> integer = new org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer>(this, java.lang.Integer.class, "integer");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, Entity1> entity1 = new org.eiichiro.acidhouse.metamodel.Property<Entity10, Entity1>(this, Entity1.class, "entity1");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<Entity1>> entity1s = new org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<Entity1>>(this, java.util.List.class, "entity1s");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, Entity1> entity12 = new org.eiichiro.acidhouse.metamodel.Property<Entity10, Entity1>(this, Entity1.class, "entity12");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, Object1> object1 = new org.eiichiro.acidhouse.metamodel.Property<Entity10, Object1>(this, Object1.class, "object1");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<Object1>> object1s = new org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<Object1>>(this, java.util.List.class, "object1s");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, Embedded2> embedded2 = new org.eiichiro.acidhouse.metamodel.Property<Entity10, Embedded2>(this, Embedded2.class, "embedded2");

		public org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<Embedded2>> embedded2s = new org.eiichiro.acidhouse.metamodel.Property<Entity10, java.util.List<Embedded2>>(this, java.util.List.class, "embedded2s");

	}

	public static class Embedded2$ extends org.eiichiro.acidhouse.metamodel.EmbeddedProperty<Entity10, Embedded2> {

		private Embedded2$(Metamodel<Entity10> parent, String name) {
			super(parent, Embedded2.class, name);
		}

		public org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer> i = new org.eiichiro.acidhouse.metamodel.ComparableProperty<Entity10, java.lang.Integer>(this, java.lang.Integer.class, "i");

	}

}
