package org.eiichiro.acidhouse.metamodel.processing;

import java.util.List;
import java.util.Set;

import org.eiichiro.acidhouse.Embedded;
import org.eiichiro.acidhouse.Key;
import org.eiichiro.acidhouse.Transient;

@Embedded
public class Embedded1 {

	@Key long key;
	
	@Transient Set<Long> longs;
	
	long[] ls;
	
	List<String> strings;
	
	int i;
	
	Integer integer;
	
	Entity2 entity2;
	
	Object1 object1;
	
	Embedded1 embedded1;
	
}
