package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;

import org.eiichiro.acidhouse.metamodel.Metamodels;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryRestrictionTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRestrictedFilterOfQ() {
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		assertTrue(new QueryRestriction().restricted(_Entity3.entity1.key.equalTo("")));
		
		QueryRestriction restriction = new QueryRestriction();
		assertFalse(restriction.restricted(_Entity3.key.greaterThan("")));
		assertTrue(restriction.restricted(_Entity3.i.greaterThan(0)));
		
		restriction = new QueryRestriction();
		assertFalse(restriction.restricted(_Entity3.key.equalTo("")));
		assertFalse(restriction.restricted(_Entity3.i.equalTo(0)));
		
		restriction = new QueryRestriction();
		assertFalse(restriction.restricted(_Entity3.key.greaterThan("")));
		assertFalse(restriction.restricted(_Entity3.key.lessThan("")));
		
		restriction = new QueryRestriction();
		assertFalse(restriction.restricted(_Entity3.key.in("", "")));
		assertFalse(restriction.restricted(_Entity3.i.in(0, 0)));
	}

	@Test
	public void testRestrictedOrderOfQQ() {
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		assertTrue(new QueryRestriction().restricted(_Entity3.entity1.key.asc));
		
		QueryRestriction restriction = new QueryRestriction();
		assertTrue(restriction.restricted(_Entity3.entity1.key.asc));
		assertTrue(restriction.restricted(_Entity3.i.asc));
		
		restriction = new QueryRestriction();
		assertFalse(restriction.restricted(_Entity3.key.greaterThan("")));
		assertTrue(restriction.restricted(_Entity3.i.asc));
	}

}
