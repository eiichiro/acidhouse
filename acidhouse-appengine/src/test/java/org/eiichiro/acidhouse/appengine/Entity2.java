package org.eiichiro.acidhouse.appengine;

import java.util.List;

import org.eiichiro.acidhouse.Entity;
import org.eiichiro.acidhouse.Key;

@Entity
public class Entity2 {

	@Key String key;
	
	List<Integer> integers;
	
}
