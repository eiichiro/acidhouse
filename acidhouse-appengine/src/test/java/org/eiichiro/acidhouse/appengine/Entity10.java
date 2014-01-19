package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;

@Entity
public class Entity10 {

	@Key public String key;
	
	public int i;
	
	Embedded1 embedded1;
	
	Embedded1 embedded12;
	
	Embedded2 embedded2;
	
}
