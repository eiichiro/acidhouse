package org.eiichiro.acidhouse.appengine;

import java.util.List;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;

@Entity
public class Entity4 {

	@Key String key;
	
	int i;
	
	List<Entity1> entity1s;
	
}
