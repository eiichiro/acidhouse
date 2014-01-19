package org.eiichiro.acidhouse.appengine;

import java.util.List;
import java.util.Set;

import org.eiichiro.acidhouse.Embedded;
import org.eiichiro.acidhouse.Referential;
import org.eiichiro.acidhouse.Transient;

@Embedded
public class Embedded1 {

	@Transient Set<Long> longs;
	
	// Unsupported
	long[] ls;
	
	List<Integer> integers;
	
	int i;
	
	Integer integer;
	
	// Unsupported
	Entity1 entity1;
	
	// Unsupported
	List<Entity1> entity1s;
	
	// Unsupported
	@Referential Entity1 entity12;
	
	// Unsupported
	Object1 object1;
	
	// Unsupported
	List<Object1> object1s;
	
	// Unsupported
	Embedded2 embedded2;
	
	// Unsupported
	List<Embedded2> embedded2s;
	
}
