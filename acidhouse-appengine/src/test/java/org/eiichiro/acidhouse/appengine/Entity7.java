package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;
import org.eiichiro.acidhouse.Referential;

@Entity
public class Entity7 {

	@Key String key;
	
	int i;
	
	@Referential Entity1 entity1;

}
