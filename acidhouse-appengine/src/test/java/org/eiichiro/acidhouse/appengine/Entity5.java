package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;

@Entity
public class Entity5 {

	@Key String key;
	
	int i;
	
	// Unsupported
	Entity1[] entity1s;
	
}
