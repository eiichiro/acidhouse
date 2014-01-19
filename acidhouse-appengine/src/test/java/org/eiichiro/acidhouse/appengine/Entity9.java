package org.eiichiro.acidhouse.appengine;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;

@Entity
public class Entity9 {

	public Entity9() {
		i = -1;
	}
	
	public Entity9(int i) {
		this.i = i;
	}
	
	@Key public String key;
	
	public final int i;
	
}
