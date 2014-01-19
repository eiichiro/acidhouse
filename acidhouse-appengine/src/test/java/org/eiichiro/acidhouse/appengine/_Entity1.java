package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.metamodel.ComparableProperty;
import org.eiichiro.acidhouse.metamodel.Metamodel;

public class _Entity1 extends Metamodel<Entity1> {

	public _Entity1() {
		super(Entity1.class);
	}

	public _Entity1(Metamodel<?> parent, String name) {
		super(parent, Entity1.class, name);
	}
	
	public ComparableProperty<Entity1, Integer> i = new ComparableProperty<Entity1, Integer>(this, Integer.class, "i");

	public ComparableProperty<Entity1, String> key = new ComparableProperty<Entity1, String>(this, String.class, "key");
	
}
