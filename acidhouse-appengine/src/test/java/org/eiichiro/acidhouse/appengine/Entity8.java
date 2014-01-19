package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;
import org.eiichiro.acidhouse.Transient;

@Entity
public class Entity8 {

	@Key String key;
	
	@Transient int i;
	
	@Transient Entity1 entity1;

}
