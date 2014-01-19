package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.eiichiro.acidhouse.Aggregations;
import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.Order;
import org.eiichiro.acidhouse.Transaction;
import org.eiichiro.acidhouse.metamodel.Metamodels;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class AppEngineGetScalarTest {

	private LocalServiceTestHelper helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
	
	@Before
	public void setUp() throws Exception {
		helper.setUp();
	}

	@After
	public void tearDown() throws Exception {
		helper.tearDown();
	}

	@Test
	public void testAppEngineGetScalar() {}

	@Test
	public void testExecute() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		
		for (int i = 0; i < 5; i++) {
			Entity3 entity3 = new Entity3();
			entity3.key = "Key" + i;
			entity3.i = i;
			Entity1 entity1 = new Entity1();
			entity1.key = "Key" + (i + 10);
			entity1.i = i + 10;
			entity3.entity1 = entity1;
			long start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity3);
			transaction.commit();
			System.out.println("AppEngineGetScalar.execute (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		long start = System.currentTimeMillis();
		int max = session.get(Aggregations.max(_Entity3.i)).execute();
		System.out.println("AppEngineGetScalar.execute #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(max, is(4));
		start = System.currentTimeMillis();
		max = session.get(Aggregations.max(_Entity3.entity1.i)).execute();
		System.out.println("AppEngineGetScalar.execute #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(max, is(14));
		
		start = System.currentTimeMillis();
		int min = session.get(Aggregations.min(_Entity3.i)).execute();
		System.out.println("AppEngineGetScalar.execute #3 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(min, is(0));
		start = System.currentTimeMillis();
		min = session.get(Aggregations.min(_Entity3.entity1.i)).execute();
		System.out.println("AppEngineGetScalar.execute #4 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(min, is(10));
		
		start = System.currentTimeMillis();
		int sum = session.get(Aggregations.sum(_Entity3.i)).execute();
		System.out.println("AppEngineGetScalar.execute #5 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(sum, is(10));
		start = System.currentTimeMillis();
		sum = session.get(Aggregations.sum(_Entity3.entity1.i)).execute();
		System.out.println("AppEngineGetScalar.execute #6 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(sum, is(60));
	}

	@Test
	public void testSort() {
		try {
			Order<?, ?> order = null;
			new AppEngineDatastoreSession().get(Metamodels.metamodel(Entity3.class)).sort(order);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testFilter() {
		try {
			Filter<?> filter = null;
			new AppEngineDatastoreSession().get(Metamodels.metamodel(Entity3.class)).filter(filter);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

}
