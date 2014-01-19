package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;

@Entity
public class Entity3 {

	@Key String key;
	
	int i;
	
	Entity1 entity1;
	
}
