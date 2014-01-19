package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.metamodel.ComparableProperty;
import org.eiichiro.acidhouse.metamodel.Metamodel;

public class _Entity3 extends Metamodel<Entity3> {

	public _Entity3() {
		super(Entity3.class);
	}

	public _Entity3(Metamodel<?> parent, String name) {
		super(parent, Entity3.class, name);
	}

	public _Entity1 entity1 = new _Entity1(this, "entity1");

	public ComparableProperty<Entity3, Integer> i = new ComparableProperty<Entity3, Integer>(this, Integer.class, "i");

	public ComparableProperty<Entity3, String> key = new ComparableProperty<Entity3, String>(this, String.class, "key");
	
}
