package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.Order;
import org.eiichiro.acidhouse.Transaction;
import org.eiichiro.acidhouse.metamodel.Metamodels;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class AppEngineGetListTest {

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
	public void testAppEngineGetList() {}

	@Test
	public void testExecute() {}

	@Test
	public void testSort() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		
		for (int i = 0; i < 5; i++) {
			Entity3 entity3 = new Entity3();
			entity3.key = "Key" + i;
			entity3.i = i;
			Entity1 entity1 = new Entity1();
			entity1.key = "Key" + (i + 10);
			entity1.i = 10 - i;
			entity3.entity1 = entity1;
			long start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity3);
			transaction.commit();
			System.out.println("AppEngineGetList.sort (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		long start = System.currentTimeMillis();
		List<Entity3> entity3s = session.get(_Entity3)
				.sort(_Entity3.i.desc)
				.execute();
		System.out.println("AppEngineGetList.sort #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.get(0).key, is("Key4"));
		assertThat(entity3s.get(4).key, is("Key0"));
		start = System.currentTimeMillis();
		entity3s = session.get(_Entity3)
				.sort(_Entity3.entity1.i.asc)
				.execute();
		System.out.println("AppEngineGetList.sort #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.get(0).key, is("Key4"));
		assertThat(entity3s.get(4).key, is("Key0"));
		start = System.currentTimeMillis();
		entity3s = session.get(_Entity3)
				.sort(_Entity3.entity1.i.desc)
				.execute();
		System.out.println("AppEngineGetList.sort #3 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.get(0).key, is("Key0"));
		assertThat(entity3s.get(4).key, is("Key4"));
		start = System.currentTimeMillis();
		entity3s = session.get(_Entity3)
				.sort(_Entity3.entity1.i.desc, _Entity3.i.desc)
				.execute();
		System.out.println("AppEngineGetList.sort #4 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.get(0).key, is("Key0"));
		assertThat(entity3s.get(4).key, is("Key4"));
		
		try {
			Order<?, ?> order = null;
			session.get(_Entity3).sort(order);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		for (int j = 0; j < 5; j++) {
			Entity10 entity10 = new Entity10();
			entity10.key = "Key" + j;
			entity10.embedded1 = new Embedded1();
			entity10.embedded1.i = j;
			entity10.embedded12 = new Embedded1();
			entity10.embedded12.i = j + 2;
			start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity10);
			transaction.commit();
			System.out.println("AppEngineGetList.sort (Setup) #" + (j + 6) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity10 _Entity10 = Metamodels.metamodel(Entity10.class);
		start = System.currentTimeMillis();
		List<Entity10> entity10s = session.get(_Entity10)
				.filter(_Entity10.embedded1.i.greaterThanOrEqualTo(3))
				.sort(_Entity10.embedded1.i.desc)
				.execute();
		System.out.println("AppEngineGetList.filter #5 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity10s.size(), is(2));
		assertThat(entity10s.get(0).key, is("Key4"));
		assertThat(entity10s.get(1).key, is("Key3"));
		
		start = System.currentTimeMillis();
		entity10s = session.get(_Entity10)
				.filter(_Entity10.embedded1.i.greaterThan(1), 
						_Entity10.embedded12.i.lessThan(6))
				.sort(_Entity10.embedded12.i.desc)
				.execute();
		System.out.println("AppEngineGetList.filter #6 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity10s.size(), is(2));
		assertThat(entity10s.get(0).key, is("Key3"));
		assertThat(entity10s.get(1).key, is("Key2"));
	}

	@Test
	public void testFilter() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		
		for (int i = 0; i < 5; i++) {
			Entity1 entity1 = new Entity1();
			entity1.key = "Key" + i;
			entity1.i = i;
			long start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity1);
			transaction.commit();
			System.out.println("AppEngineGetList.filter (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		int i = 0;
		_Entity1 _Entity1 = Metamodels.metamodel(Entity1.class);
		long start = System.currentTimeMillis();
		List<Entity1> entity1s = session.get(_Entity1)
				.filter(_Entity1.i.equalTo(0))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(1));
		assertThat(entity1s.get(0).key, is("Key0"));
		assertThat(entity1s.get(0).i, is(0));
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1)
				.filter(_Entity1.i.greaterThan(2))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(2));
		assertThat(entity1s.get(0).key, is("Key3"));
		assertThat(entity1s.get(0).i, is(3));
		assertThat(entity1s.get(1).key, is("Key4"));
		assertThat(entity1s.get(1).i, is(4));
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1)
				.filter(_Entity1.i.greaterThanOrEqualTo(2))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(3));
		assertThat(entity1s.get(0).key, is("Key2"));
		assertThat(entity1s.get(0).i, is(2));
		assertThat(entity1s.get(1).key, is("Key3"));
		assertThat(entity1s.get(1).i, is(3));
		assertThat(entity1s.get(2).key, is("Key4"));
		assertThat(entity1s.get(2).i, is(4));
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1)
				.filter(_Entity1.i.lessThan(2))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(2));
		assertThat(entity1s.get(0).key, is("Key0"));
		assertThat(entity1s.get(0).i, is(0));
		assertThat(entity1s.get(1).key, is("Key1"));
		assertThat(entity1s.get(1).i, is(1));
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1)
				.filter(_Entity1.i.lessThanOrEqualTo(2))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(3));
		assertThat(entity1s.get(0).key, is("Key0"));
		assertThat(entity1s.get(0).i, is(0));
		assertThat(entity1s.get(1).key, is("Key1"));
		assertThat(entity1s.get(1).i, is(1));
		assertThat(entity1s.get(2).key, is("Key2"));
		assertThat(entity1s.get(2).i, is(2));
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1)
				.filter(_Entity1.i.in(1, 2, 3))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(3));
		assertThat(entity1s.get(0).key, is("Key1"));
		assertThat(entity1s.get(0).i, is(1));
		assertThat(entity1s.get(1).key, is("Key2"));
		assertThat(entity1s.get(1).i, is(2));
		assertThat(entity1s.get(2).key, is("Key3"));
		assertThat(entity1s.get(2).i, is(3));
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1)
				.filter(_Entity1.key.in("Key1", "Key2", "Key3"))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(3));
		assertThat(entity1s.get(0).key, is("Key1"));
		assertThat(entity1s.get(0).i, is(1));
		assertThat(entity1s.get(1).key, is("Key2"));
		assertThat(entity1s.get(1).i, is(2));
		assertThat(entity1s.get(2).key, is("Key3"));
		assertThat(entity1s.get(2).i, is(3));
		
		for (int j = 0; j < 5; j++) {
			Entity3 entity3 = new Entity3();
			entity3.key = "Key" + j;
			entity3.i = j;
			Entity1 entity1 = new Entity1();
			entity1.key = "Key" + (j + 10);
			entity1.i = j + 10;
			entity3.entity1 = entity1;
			start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity3);
			transaction.commit();
			System.out.println("AppEngineGetList.filter (Setup) #" + (j + 6) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		start = System.currentTimeMillis();
		List<Entity3> entity3s = session.get(_Entity3)
				.filter(_Entity3.entity1.i.equalTo(10))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.size(), is(1));
		assertThat(entity3s.get(0).key, is("Key0"));
		assertThat(entity3s.get(0).i, is(0));
		assertThat(entity3s.get(0).entity1.key, is("Key10"));
		assertThat(entity3s.get(0).entity1.i, is(10));
		
		start = System.currentTimeMillis();
		entity3s = session.get(_Entity3)
				.filter(_Entity3.entity1.i.greaterThan(11), 
						_Entity3.entity1.i.lessThan(14))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.size(), is(2));
		assertThat(entity3s.get(0).key, is("Key2"));
		assertThat(entity3s.get(0).i, is(2));
		assertThat(entity3s.get(0).entity1.key, is("Key12"));
		assertThat(entity3s.get(0).entity1.i, is(12));
		assertThat(entity3s.get(1).key, is("Key3"));
		assertThat(entity3s.get(1).i, is(3));
		assertThat(entity3s.get(1).entity1.key, is("Key13"));
		assertThat(entity3s.get(1).entity1.i, is(13));
		
		start = System.currentTimeMillis();
		entity3s = session.get(_Entity3)
				.filter(_Entity3.entity1.i.greaterThanOrEqualTo(12), 
						_Entity3.entity1.i.lessThanOrEqualTo(13))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.size(), is(2));
		assertThat(entity3s.get(0).key, is("Key2"));
		assertThat(entity3s.get(0).i, is(2));
		assertThat(entity3s.get(0).entity1.key, is("Key12"));
		assertThat(entity3s.get(0).entity1.i, is(12));
		assertThat(entity3s.get(1).key, is("Key3"));
		assertThat(entity3s.get(1).i, is(3));
		assertThat(entity3s.get(1).entity1.key, is("Key13"));
		assertThat(entity3s.get(1).entity1.i, is(13));
		
		start = System.currentTimeMillis();
		entity3s = session.get(_Entity3)
				.filter(_Entity3.entity1.i.greaterThanOrEqualTo(11), 
						_Entity3.entity1.i.in(12, 13))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3s.size(), is(2));
		assertThat(entity3s.get(0).key, is("Key2"));
		assertThat(entity3s.get(0).i, is(2));
		assertThat(entity3s.get(0).entity1.key, is("Key12"));
		assertThat(entity3s.get(0).entity1.i, is(12));
		assertThat(entity3s.get(1).key, is("Key3"));
		assertThat(entity3s.get(1).i, is(3));
		assertThat(entity3s.get(1).entity1.key, is("Key13"));
		assertThat(entity3s.get(1).entity1.i, is(13));
		
		try {
			Filter<?> filter = null;
			session.get(_Entity3).filter(filter);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		for (int k = 0; k < 5; k++) {
			Entity10 entity10 = new Entity10();
			entity10.key = "Key" + k;
			entity10.i = 10;
			entity10.embedded1 = new Embedded1();
			entity10.embedded1.longs = new HashSet<Long>();
			entity10.embedded1.longs.add(1011L);
			entity10.embedded1.ls = new long[] {1012L};
			entity10.embedded1.integers = new ArrayList<Integer>();
			entity10.embedded1.integers.add(1013);
			entity10.embedded1.i = k;
			entity10.embedded1.integer = k + 1;
			entity10.embedded1.entity1 = new Entity1();
			entity10.embedded1.entity1.key = "Key1011";
			entity10.embedded1.entity1s = new ArrayList<Entity1>();
			entity10.embedded1.entity1s.add(new Entity1());
			entity10.embedded1.entity1s.get(0).key = "Key1012";
			entity10.embedded1.entity12 = new Entity1();
			entity10.embedded1.entity12.key = "Key1013";
			entity10.embedded1.object1 = new Object1();
			entity10.embedded1.object1s = new ArrayList<Object1>();
			entity10.embedded1.object1s.add(new Object1());
			entity10.embedded1.embedded2 = new Embedded2();
			entity10.embedded1.embedded2.i = 1016;
			entity10.embedded1.embedded2s = new ArrayList<Embedded2>();
			entity10.embedded1.embedded2s.add(new Embedded2());
			entity10.embedded1.embedded2s.get(0).i = 1017;
			entity10.embedded12 = new Embedded1();
			entity10.embedded12.longs = new HashSet<Long>();
			entity10.embedded12.longs.add(1021L);
			entity10.embedded12.ls = new long[] {1022L};
			entity10.embedded12.integers = new ArrayList<Integer>();
			entity10.embedded12.integers.add(1023);
			entity10.embedded12.i = k + 2;
			entity10.embedded12.integer = k + 3;
			entity10.embedded12.entity1 = new Entity1();
			entity10.embedded12.entity1.key = "Key1021";
			entity10.embedded12.entity1s = new ArrayList<Entity1>();
			entity10.embedded12.entity1s.add(new Entity1());
			entity10.embedded12.entity1s.get(0).key = "Key1022";
			entity10.embedded12.entity12 = new Entity1();
			entity10.embedded12.entity12.key = "Key1023";
			entity10.embedded12.object1 = new Object1();
			entity10.embedded12.object1s = new ArrayList<Object1>();
			entity10.embedded12.object1s.add(new Object1());
			entity10.embedded12.embedded2 = new Embedded2();
			entity10.embedded12.embedded2.i = k + 4;
			entity10.embedded12.embedded2s = new ArrayList<Embedded2>();
			entity10.embedded12.embedded2s.add(new Embedded2());
			entity10.embedded12.embedded2s.get(0).i = 1027;
			entity10.embedded2 = new Embedded2();
			entity10.embedded2.i = k + 5;
			start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity10);
			transaction.commit();
			System.out.println("AppEngineGetList.filter (Setup) #" + (k + 11) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity10 _Entity10 = Metamodels.metamodel(Entity10.class);
		start = System.currentTimeMillis();
		List<Entity10> entity10s = session.get(_Entity10)
				.filter(_Entity10.embedded1.i.equalTo(1))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity10s.size(), is(1));
		assertThat(entity10s.get(0).key, is("Key1"));
		
		start = System.currentTimeMillis();
		entity10s = session.get(_Entity10)
				.filter(_Entity10.embedded1.i.greaterThan(1), 
						_Entity10.embedded12.i.lessThan(6))
				.execute();
		System.out.println("AppEngineGetList.filter #" + ++i + " [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity10s.size(), is(2));
		assertThat(entity10s.get(0).key, is("Key2"));
		assertThat(entity10s.get(1).key, is("Key3"));
	}

	@Test
	public void testLimit() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		
		for (int i = 0; i < 5; i++) {
			Entity1 entity1 = new Entity1();
			entity1.key = "Key" + i;
			entity1.i = i;
			long start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity1);
			transaction.commit();
			System.out.println("AppEngineGetList.limit (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity1 _Entity1 = Metamodels.metamodel(Entity1.class);
		long start = System.currentTimeMillis();
		List<Entity1> entity1s = session.get(_Entity1).limit(2).execute();
		System.out.println("AppEngineGetList.limit #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(2));
		
		try {
			start = System.currentTimeMillis();
			entity1s = session.get(_Entity1).limit(0).execute();
			System.out.println("AppEngineGetList.limit #2 [" + (System.currentTimeMillis() - start) + "]");
			fail();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1).limit(10).execute();
		System.out.println("AppEngineGetList.limit #3 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(5));
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1).offset(2).limit(3).execute();
		System.out.println("AppEngineGetList.limit #4 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(3));
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1).offset(1).limit(10).execute();
		System.out.println("AppEngineGetList.limit #5 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(4));
	}

	@Test
	public void testOffset() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		
		for (int i = 0; i < 5; i++) {
			Entity1 entity1 = new Entity1();
			entity1.key = "Key" + i;
			entity1.i = i;
			long start = System.currentTimeMillis();
			Transaction transaction = session.beginTransaction();
			session.put(entity1);
			transaction.commit();
			System.out.println("AppEngineGetList.offset (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity1 _Entity1 = Metamodels.metamodel(Entity1.class);
		long start = System.currentTimeMillis();
		List<Entity1> entity1s = session.get(_Entity1).offset(3).execute();
		System.out.println("AppEngineGetList.offset #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(2));
		
		try {
			start = System.currentTimeMillis();
			entity1s = session.get(_Entity1).offset(-1).execute();
			System.out.println("AppEngineGetList.offset #2 [" + (System.currentTimeMillis() - start) + "]");
			fail();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1).offset(10).execute();
		System.out.println("AppEngineGetList.offset #3 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(0));
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1).offset(2).limit(3).execute();
		System.out.println("AppEngineGetList.offset #4 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(3));
		start = System.currentTimeMillis();
		entity1s = session.get(_Entity1).offset(10).limit(10).execute();
		System.out.println("AppEngineGetList.offset #5 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1s.size(), is(0));
	}

}
