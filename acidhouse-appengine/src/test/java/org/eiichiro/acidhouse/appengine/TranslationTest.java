package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.eiichiro.acidhouse.ComparableFilter.Operator;
import org.eiichiro.acidhouse.Lock;
import org.eiichiro.acidhouse.Log;
import org.eiichiro.acidhouse.Order.Direction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class TranslationTest {

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
	public void testToObject() {
		AppEngineDatastoreService datastore = new AppEngineDatastoreService(DatastoreServiceFactory.getDatastoreService());
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		List<Entity> entities = Translation.toEntities(entity1);
		Map<Key, Object> references = new HashMap<Key, Object>();
		long start = System.currentTimeMillis();
		entity1 = Translation.toObject(Entity1.class, entities, references, datastore);
		System.out.println("Translation.toObject #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1.key, is("Key1"));
		assertThat(entity1.i, is(1));
		
		Entity2 entity2 = new Entity2();
		entity2.key = "Key2";
		List<Integer> integers = new ArrayList<Integer>();
		integers.add(1);
		integers.add(2);
		integers.add(3);
		entity2.integers = integers;
		entities = Translation.toEntities(entity2);
		references = new HashMap<Key, Object>();
		start = System.currentTimeMillis();
		entity2 = Translation.toObject(Entity2.class, entities, references, datastore);
		System.out.println("Translation.toObject #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity2.key, is("Key2"));
		assertThat(entity2.integers.size(), is(3));
		assertThat(entity2.integers.get(0), is(1));
		assertThat(entity2.integers.get(1), is(2));
		assertThat(entity2.integers.get(2), is(3));
		
		Entity3 entity3 = new Entity3();
		entity3.key = "Key3";
		entity3.i = 3;
		entity3.entity1 = new Entity1();
		entity3.entity1.key = "Key31";
		entity3.entity1.i = 31;
		entities = Translation.toEntities(entity3);
		references = new HashMap<Key, Object>();
		start = System.currentTimeMillis();
		entity3 = Translation.toObject(Entity3.class, entities, references, datastore);
		System.out.println("Translation.toObject #3 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3.key, is("Key3"));
		assertThat(entity3.i, is(3));
		assertThat(entity3.entity1.key, is("Key31"));
		assertThat(entity3.entity1.i, is(31));
		
		Entity4 entity4 = new Entity4();
		entity4.key = "Key4";
		entity4.i = 4;
		entity4.entity1s = new ArrayList<Entity1>();
		Entity1 entity41 = new Entity1();
		entity41.key = "Key41";
		entity41.i = 41;
		Entity1 entity42 = new Entity1();
		entity42.key = "Key42";
		entity42.i = 42;
		Entity1 entity43 = new Entity1();
		entity43.key = "Key43";
		entity43.i = 43;
		entity4.entity1s.add(entity41);
		entity4.entity1s.add(entity42);
		entity4.entity1s.add(entity43);
		entities = Translation.toEntities(entity4);
		references = new HashMap<Key, Object>();
		start = System.currentTimeMillis();
		entity4 = Translation.toObject(Entity4.class, entities, references, datastore);
		System.out.println("Translation.toObject #4 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity4.key, is("Key4"));
		assertThat(entity4.i, is(4));
		assertThat(entity4.entity1s.size(), is(3));
		assertThat(entity4.entity1s.get(0).key, is("Key41"));
		assertThat(entity4.entity1s.get(0).i, is(41));
		assertThat(entity4.entity1s.get(1).key, is("Key42"));
		assertThat(entity4.entity1s.get(1).i, is(42));
		assertThat(entity4.entity1s.get(2).key, is("Key43"));
		assertThat(entity4.entity1s.get(2).i, is(43));
		
		entities = new ArrayList<Entity>();
		Entity entity = new Entity(KeyFactory.createKey(Entity5.class.getSimpleName(), "Key5"));
		entity.setProperty("i", 1);
		entities.add(entity);
		references = new HashMap<Key, Object>();
		Entity5 entity5 = Translation.toObject(Entity5.class, entities, references, datastore);
		assertThat(entity5.key, is("Key5"));
		assertThat(entity5.i, is(1));
		assertNull(entity5.entity1s);
		
		entities = new ArrayList<Entity>();
		entities.add(new Entity(KeyFactory.createKey(Entity6.class.getSimpleName(), "Key6")));
		references = new HashMap<Key, Object>();
		start = System.currentTimeMillis();
		Translation.toObject(Entity6.class, entities, references, datastore);
		System.out.println("Translation.toObject #6 [" + (System.currentTimeMillis() - start) + "]");
		
		Transaction transaction = datastore.beginTransaction();
		datastore.put(transaction, Translation.toEntities(entity1));
		transaction.commit();
		Entity7 entity71 = new Entity7();
		entity71.key = "Key71";
		entity71.i = 71;
		entity71.entity1 = entity1;
		Entity7 entity72 = new Entity7();
		entity72.key = "Key72";
		entity72.i = 72;
		entity72.entity1 = entity1;
		
		references = new HashMap<Key, Object>();
		entities = Translation.toEntities(entity71);
		start = System.currentTimeMillis();
		entity71 = Translation.toObject(Entity7.class, entities, references, datastore);
		System.out.println("Translation.toObject #7-1 [" + (System.currentTimeMillis() - start) + "]");
		entities = Translation.toEntities(entity72);
		start = System.currentTimeMillis();
		entity72 = Translation.toObject(Entity7.class, entities, references, datastore);
		System.out.println("Translation.toObject #7-2 [" + (System.currentTimeMillis() - start) + "]");
		assertSame(entity71.entity1, entity72.entity1);
		assertThat(references.size(), is(1));
		assertSame(entity71.entity1, references.values().toArray()[0]);
		
		Entity8 entity8 = new Entity8();
		entity8.key = "Key8";
		entity8.i = 8;
		entity8.entity1 = entity1;
		references = new HashMap<Key, Object>();
		entities = Translation.toEntities(entity8);
		start = System.currentTimeMillis();
		entity8 = Translation.toObject(Entity8.class, entities, references, datastore);
		System.out.println("Translation.toObject #8 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity8.key, is("Key8"));
		assertThat(entity8.i, is(0));
		assertNull(entity8.entity1);
		
		Entity9 entity9 = new Entity9(9);
		entity9.key = "Key9";
		references = new HashMap<Key, Object>();
		entities = Translation.toEntities(entity9);
		start = System.currentTimeMillis();
		entity9 = Translation.toObject(Entity9.class, entities, references, datastore);
		System.out.println("Translation.toObject #9 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity9.key, is("Key9"));
		assertThat(entity9.i, is(-1));
		
		Entity10 entity10 = new Entity10();
		entity10.key = "Key10";
		entity10.i = 10;
		entity10.embedded1 = new Embedded1();
		entity10.embedded1.longs = new HashSet<Long>();
		entity10.embedded1.longs.add(1011L);
		entity10.embedded1.ls = new long[] {1012L};
		entity10.embedded1.integers = new ArrayList<Integer>();
		entity10.embedded1.integers.add(1013);
		entity10.embedded1.i = 1014;
		entity10.embedded1.integer = 1015;
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
		entity10.embedded12.i = 1024;
		entity10.embedded12.integer = 1025;
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
		entity10.embedded12.embedded2.i = 1026;
		entity10.embedded12.embedded2s = new ArrayList<Embedded2>();
		entity10.embedded12.embedded2s.add(new Embedded2());
		entity10.embedded12.embedded2s.get(0).i = 1027;
		entity10.embedded2 = new Embedded2();
		entity10.embedded2.i = 1031;
		references = new HashMap<Key, Object>();
		entities = Translation.toEntities(entity10);
		start = System.currentTimeMillis();
		entity10 = Translation.toObject(Entity10.class, entities, references, datastore);
		System.out.println("Translation.toObject #10 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		assertThat(entity10.key, is("Key10"));
		assertThat(entity10.i, is(10));
		assertTrue(entity10.embedded1.longs.isEmpty());
		assertNull(entity10.embedded1.ls);
		assertThat(entity10.embedded1.integers.size(), is(1));
		assertThat(entity10.embedded1.integers.get(0), is(1013));
		assertThat(entity10.embedded1.i, is(1014));
		assertThat(entity10.embedded1.integer, is(1015));
		assertNull(entity10.embedded1.entity1);
		assertTrue(entity10.embedded1.entity1s.isEmpty());
		assertNull(entity10.embedded1.object1);
		assertTrue(entity10.embedded1.object1s.isEmpty());
		assertNull(entity10.embedded1.embedded2);
		assertTrue(entity10.embedded1.embedded2s.isEmpty());
		assertTrue(entity10.embedded12.longs.isEmpty());
		assertNull(entity10.embedded12.ls);
		assertThat(entity10.embedded12.integers.size(), is(1));
		assertThat(entity10.embedded12.integers.get(0), is(1023));
		assertThat(entity10.embedded12.i, is(1024));
		assertThat(entity10.embedded12.integer, is(1025));
		assertNull(entity10.embedded12.entity1);
		assertTrue(entity10.embedded12.entity1s.isEmpty());
		assertNull(entity10.embedded12.object1);
		assertTrue(entity10.embedded12.object1s.isEmpty());
		assertNull(entity10.embedded12.embedded2);
		assertTrue(entity10.embedded12.embedded2s.isEmpty());
		assertThat(entity10.embedded2.i, is(1031));
		
		entity10 = new Entity10();
		entity10.key = "Key10";
		entity10.embedded1 = new Embedded1();
		entity10.embedded12 = null;
		entities = Translation.toEntities(entity10);
		start = System.currentTimeMillis();
		entity10 = Translation.toObject(Entity10.class, entities, references, datastore);
		System.out.println("Translation.toObject #11 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		assertThat(entity10.key, is("Key10"));
		assertThat(entity10.i, is(0));
		assertTrue(entity10.embedded1.longs.isEmpty());
		assertNull(entity10.embedded1.ls);
		assertTrue(entity10.embedded1.integers.isEmpty());
		assertThat(entity10.embedded1.i, is(0));
		assertNull(entity10.embedded1.integer);
		assertNull(entity10.embedded1.entity1);
		assertTrue(entity10.embedded1.entity1s.isEmpty());
		assertNull(entity10.embedded1.object1);
		assertTrue(entity10.embedded1.object1s.isEmpty());
		assertNull(entity10.embedded1.embedded2);
		assertTrue(entity10.embedded1.embedded2s.isEmpty());
		assertTrue(entity10.embedded12.longs.isEmpty());
		assertNull(entity10.embedded12.ls);
		assertTrue(entity10.embedded12.integers.isEmpty());
		assertThat(entity10.embedded12.i, is(0));
		assertNull(entity10.embedded12.integer);
		assertNull(entity10.embedded12.entity1);
		assertTrue(entity10.embedded12.entity1s.isEmpty());
		assertNull(entity10.embedded12.object1);
		assertTrue(entity10.embedded12.object1s.isEmpty());
		assertNull(entity10.embedded12.embedded2);
		assertTrue(entity10.embedded12.embedded2s.isEmpty());
		assertThat(entity10.embedded2.i, is(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToEntitiesObject() {
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		long start = System.currentTimeMillis();
		List<Entity> entities = Translation.toEntities(entity1);
		System.out.println("Translation.toEntities #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		Entity entities0 = entities.get(0);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity1.class), "Key1")));
		assertThat((Integer) entities0.getProperty("i"), is(1));
		
		Entity2 entity2 = new Entity2();
		entity2.key = "Key2";
		List<Integer> integers = new ArrayList<Integer>();
		integers.add(1);
		integers.add(2);
		integers.add(3);
		entity2.integers = integers;
		start = System.currentTimeMillis();
		entities = Translation.toEntities(entity2);
		System.out.println("Translation.toEntities #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		entities0 = entities.get(0);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity2.class), "Key2")));
		List<Integer> list = (List<Integer>) entities0.getProperty("integers");
		assertThat(list.size(), is(3));
		assertThat(list.get(0), is(1));
		assertThat(list.get(1), is(2));
		assertThat(list.get(2), is(3));
		
		Entity3 entity3 = new Entity3();
		entity3.key = "Key3";
		entity3.i = 3;
		entity3.entity1 = new Entity1();
		entity3.entity1.key = "Key31";
		entity3.entity1.i = 31;
		start = System.currentTimeMillis();
		entities = Translation.toEntities(entity3);
		System.out.println("Translation.toEntities #3 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(2));
		entities0 = entities.get(0);
		Entity entities1 = entities.get(1);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity3.class), "Key3")));
		assertThat((Integer) entities0.getProperty("i"), is(3));
		assertThat(entities1.getKey(), is(KeyFactory.createKey(entities0.getKey(), Translation.toKind(Entity1.class), "Key31")));
		assertThat((Integer) entities1.getProperty("i"), is(31));
		
		Entity4 entity4 = new Entity4();
		entity4.key = "Key4";
		entity4.i = 4;
		entity4.entity1s = new ArrayList<Entity1>();
		Entity1 entity41 = new Entity1();
		entity41.key = "Key41";
		entity41.i = 41;
		Entity1 entity42 = new Entity1();
		entity42.key = "Key42";
		entity42.i = 42;
		Entity1 entity43 = new Entity1();
		entity43.key = "Key43";
		entity43.i = 43;
		entity4.entity1s.add(entity41);
		entity4.entity1s.add(entity42);
		entity4.entity1s.add(entity43);
		start = System.currentTimeMillis();
		entities = Translation.toEntities(entity4);
		System.out.println("Translation.toEntities #4 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(4));
		entities0 = entities.get(0);
		entities1 = entities.get(1);
		Entity entities2 = entities.get(2);
		Entity entities3 = entities.get(3);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity4.class), "Key4")));
		assertThat((Integer) entities0.getProperty("i"), is(4));
		assertThat(entities1.getKey(), is(KeyFactory.createKey(entities0.getKey(), Translation.toKind(Entity1.class), "Key41")));
		assertThat((Integer) entities1.getProperty("i"), is(41));
		assertThat(entities2.getKey(), is(KeyFactory.createKey(entities0.getKey(), Translation.toKind(Entity1.class), "Key42")));
		assertThat((Integer) entities2.getProperty("i"), is(42));
		assertThat(entities3.getKey(), is(KeyFactory.createKey(entities0.getKey(), Translation.toKind(Entity1.class), "Key43")));
		assertThat((Integer) entities3.getProperty("i"), is(43));
		
		Entity5 entity5 = new Entity5();
		entity5.key = "Key5";
		entity5.i = 5;
		entity5.entity1s = new Entity1[] {};
		entities = Translation.toEntities(entity5);
		assertThat(entities.size(), is(1));
		assertThat(entities.get(0).getKey(), is(KeyFactory.createKey(Translation.toKind(Entity5.class), "Key5")));
		assertThat((Integer) entities.get(0).getProperty("i"), is(5));
		assertNull(entities.get(0).getProperty("entity1s"));
		
		Entity6 entity6 = new Entity6();
		
		try {
			Translation.toEntities(entity6);
			fail();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Entity7 entity7 = new Entity7();
		entity7.key = "Key7";
		entity7.i = 7;
		entity7.entity1 = new Entity1();
		entity7.entity1.key = "Key71";
		entity7.entity1.i = 71;
		start = System.currentTimeMillis();
		entities = Translation.toEntities(entity7);
		System.out.println("Translation.toEntities #7 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		entities0 = entities.get(0);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity7.class), "Key7")));
		assertThat((Integer) entities0.getProperty("i"), is(7));
		assertThat((Key) entities0.getProperty("entity1"), is(KeyFactory.createKey(Translation.toKind(Entity1.class), "Key71")));
		
		Entity8 entity8 = new Entity8();
		entity8.key = "Key8";
		entity8.i = 8;
		entity8.entity1 = new Entity1();
		entity8.entity1.key = "Key81";
		entity8.entity1.i = 81;
		start = System.currentTimeMillis();
		entities = Translation.toEntities(entity8);
		System.out.println("Translation.toEntities #8 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		entities0 = entities.get(0);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity8.class), "Key8")));
		assertNull(entities0.getProperty("entity1"));
		
		Entity10 entity10 = new Entity10();
		entity10.key = "Key10";
		entity10.i = 10;
		entity10.embedded1 = new Embedded1();
		entity10.embedded1.longs = new HashSet<Long>();
		entity10.embedded1.longs.add(1011L);
		entity10.embedded1.ls = new long[] {1012L};
		entity10.embedded1.integers = new ArrayList<Integer>();
		entity10.embedded1.integers.add(1013);
		entity10.embedded1.i = 1014;
		entity10.embedded1.integer = 1015;
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
		entity10.embedded1.embedded2.i = 1015;
		entity10.embedded1.embedded2s = new ArrayList<Embedded2>();
		entity10.embedded1.embedded2s.add(new Embedded2());
		entity10.embedded1.embedded2s.get(0).i = 1016;
		entity10.embedded12 = new Embedded1();
		entity10.embedded12.longs = new HashSet<Long>();
		entity10.embedded12.longs.add(1021L);
		entity10.embedded12.ls = new long[] {1022L};
		entity10.embedded12.integers = new ArrayList<Integer>();
		entity10.embedded12.integers.add(1023);
		entity10.embedded12.i = 1024;
		entity10.embedded12.integer = 1025;
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
		entity10.embedded12.embedded2.i = 1025;
		entity10.embedded12.embedded2s = new ArrayList<Embedded2>();
		entity10.embedded12.embedded2s.add(new Embedded2());
		entity10.embedded12.embedded2s.get(0).i = 1026;
		entity10.embedded2 = new Embedded2();
		entity10.embedded2.i = 1031;
		start = System.currentTimeMillis();
		entities = Translation.toEntities(entity10);
		System.out.println("Translation.toEntities #10 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		entities0 = entities.get(0);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity10.class), "Key10")));
		assertThat((Integer) entities0.getProperty("i"), is(10));
		assertNull(entities0.getProperty("embedded1.longs"));
		assertTrue(!entities0.hasProperty("embedded1.ls"));
		assertThat(((List<Integer>) entities0.getProperty("embedded1.integers")).size(), is(1));
		assertThat(((List<Integer>) entities0.getProperty("embedded1.integers")).get(0), is(1013));
		assertThat((Integer) entities0.getProperty("embedded1.i"), is(1014));
		assertThat((Integer) entities0.getProperty("embedded1.integer"), is(1015));
		assertTrue(!entities0.hasProperty("embedded1.entity1"));
		assertTrue(!entities0.hasProperty("embedded1.entity1s"));
		assertTrue(!entities0.hasProperty("embedded1.entity12"));
		assertTrue(!entities0.hasProperty("embedded1.object1"));
		assertTrue(!entities0.hasProperty("embedded1.object1s"));
		assertTrue(!entities0.hasProperty("embedded1.embedded2"));
		assertTrue(!entities0.hasProperty("embedded1.embedded2s"));
		assertNull(entities0.getProperty("embedded12.longs"));
		assertTrue(!entities0.hasProperty("embedded12.ls"));
		assertThat(((List<Integer>) entities0.getProperty("embedded12.integers")).size(), is(1));
		assertThat(((List<Integer>) entities0.getProperty("embedded12.integers")).get(0), is(1023));
		assertThat((Integer) entities0.getProperty("embedded12.i"), is(1024));
		assertThat((Integer) entities0.getProperty("embedded12.integer"), is(1025));
		assertTrue(!entities0.hasProperty("embedded12.entity1"));
		assertTrue(!entities0.hasProperty("embedded12.entity1s"));
		assertTrue(!entities0.hasProperty("embedded12.entity12"));
		assertTrue(!entities0.hasProperty("embedded12.object1"));
		assertTrue(!entities0.hasProperty("embedded12.object1s"));
		assertTrue(!entities0.hasProperty("embedded12.embedded2"));
		assertTrue(!entities0.hasProperty("embedded12.embedded2s"));
		assertThat((Integer) entities0.getProperty("embedded2.i"), is(1031));
		
		entity10 = new Entity10();
		entity10.key = "Key10";
		entity10.embedded1 = new Embedded1();
		entity10.embedded12 = null;
		entities = Translation.toEntities(entity10);
		System.out.println("Translation.toEntities #11 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(1));
		entities0 = entities.get(0);
		assertThat(entities0.getKey(), is(KeyFactory.createKey(Translation.toKind(Entity10.class), "Key10")));
		assertThat((Integer) entities0.getProperty("i"), is(0));
		assertTrue(!entities0.hasProperty("embedded1.longs"));
		assertTrue(!entities0.hasProperty("embedded1.ls"));
		assertTrue(!entities0.hasProperty("embedded1.integers"));
		assertThat((Integer) entities0.getProperty("embedded1.i"), is(0));
		assertTrue(!entities0.hasProperty("embedded1.integer"));
		assertTrue(!entities0.hasProperty("embedded1.entity1"));
		assertTrue(!entities0.hasProperty("embedded1.entity1s"));
		assertTrue(!entities0.hasProperty("embedded1.entity12"));
		assertTrue(!entities0.hasProperty("embedded1.object1"));
		assertTrue(!entities0.hasProperty("embedded1.object1s"));
		assertTrue(!entities0.hasProperty("embedded1.embedded2"));
		assertTrue(!entities0.hasProperty("embedded1.embedded2s"));
		assertTrue(!entities0.hasProperty("embedded12.longs"));
		assertTrue(!entities0.hasProperty("embedded12.ls"));
		assertTrue(!entities0.hasProperty("embedded12.integers"));
		assertTrue(!entities0.hasProperty("embedded12.i"));
		assertTrue(!entities0.hasProperty("embedded12.integer"));
		assertTrue(!entities0.hasProperty("embedded12.entity1"));
		assertTrue(!entities0.hasProperty("embedded12.entity1s"));
		assertTrue(!entities0.hasProperty("embedded12.entity12"));
		assertTrue(!entities0.hasProperty("embedded12.object1"));
		assertTrue(!entities0.hasProperty("embedded12.object1s"));
		assertTrue(!entities0.hasProperty("embedded12.embedded2"));
		assertTrue(!entities0.hasProperty("embedded12.embedded2s"));
		assertTrue(!entities0.hasProperty("embedded2.i"));
	}

	@Test
	public void testToEntitiesKeyObject() {}

	@Test
	public void testToLock() {
		String id = UUID.randomUUID().toString();
		String transaction = UUID.randomUUID().toString();
		Date timestamp = new Date();
		Lock lock = new Lock(id, transaction, timestamp);
		Entity entity = Translation.toEntity(lock, KeyFactory.createKey(Translation.toKind(Entity1.class), "Key1"));
		lock = Translation.toLock(entity);
		assertThat(lock.id(), is(id));
		assertThat(lock.transaction(), is(transaction));
		assertThat(lock.timestamp(), is(timestamp));
	}

	@Test
	public void testToEntity() {
		String id = UUID.randomUUID().toString();
		String transaction = UUID.randomUUID().toString();
		Date timestamp = new Date();
		Lock lock = new Lock(id, transaction, timestamp);
		Entity entity = Translation.toEntity(lock, KeyFactory.createKey(Translation.toKind(Entity1.class), "Key1"));
		Key key = entity.getKey();
		assertThat(key.getKind(), is(Translation.LOCK_KIND));
		assertThat(key.getName(), is(id));
		assertThat(key.getParent(), is(KeyFactory.createKey(Translation.toKind(Entity1.class), "Key1")));
		assertThat((String) entity.getProperty(Translation.TRANSACTION_PROPERTY), is(transaction));
		assertTrue(entity.isUnindexedProperty(Translation.TRANSACTION_PROPERTY));
		assertThat((Date) entity.getProperty(Translation.TIMESTAMP_PROPERTY), is(timestamp));
		assertTrue(entity.isUnindexedProperty(Translation.TIMESTAMP_PROPERTY));
	}

	@Test
	public void testToTransaction() {
		AppEngineDatastoreService datastore = new AppEngineDatastoreService(
				DatastoreServiceFactory.getDatastoreService());
		Entity1 entity11 = new Entity1();
		entity11.key = "Key11";
		entity11.i = 11;
		Entity1 entity12 = new Entity1();
		entity12.key = "Key12";
		entity12.i = 12;
		Entity1 entity13 = new Entity1();
		entity13.key = "Key13";
		entity13.i = 13;
		Key key = Keys.create(Translation.toKind(Entity1.class), "Key11");
		String id = UUID.randomUUID().toString();
		AppEngineGlobalTransaction transaction = new AppEngineGlobalTransaction(id, null, null);
		transaction.logs().add(new Log(1, Log.Operation.PUT, entity11));
		transaction.logs().add(new Log(2, Log.Operation.UPDATE, entity12));
		transaction.logs().add(new Log(3, Log.Operation.DELETE, entity13));
		List<Entity> entities = Translation.toEntities(transaction, key);
		long start = System.currentTimeMillis();
		transaction = Translation.toTransaction(entities, datastore);
		System.out.println("Translation.toTransaction [" + (System.currentTimeMillis() - start) + "]");
		assertThat(transaction.id(), is(id));
		assertThat(transaction.logs().size(), is(3));
		assertThat(transaction.logs().get(0).sequence(), is(1L));
		assertThat(transaction.logs().get(0).operation(), is(Log.Operation.PUT));
		assertThat(((Entity1) transaction.logs().get(0).entity()).key, is("Key11"));
		assertThat(((Entity1) transaction.logs().get(0).entity()).i, is(11));
		assertThat(transaction.logs().get(1).sequence(), is(2L));
		assertThat(transaction.logs().get(1).operation(), is(Log.Operation.UPDATE));
		assertThat(((Entity1) transaction.logs().get(1).entity()).key, is("Key12"));
		assertThat(((Entity1) transaction.logs().get(1).entity()).i, is(12));
		assertThat(transaction.logs().get(2).sequence(), is(3L));
		assertThat(transaction.logs().get(2).operation(), is(Log.Operation.DELETE));
		assertThat(((Entity1) transaction.logs().get(2).entity()).key, is("Key13"));
		assertThat(((Entity1) transaction.logs().get(2).entity()).i, is(13));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToEntitiesAppEngineTransactionKey() {
		Entity1 entity11 = new Entity1();
		entity11.key = "Key11";
		entity11.i = 11;
		Entity1 entity12 = new Entity1();
		entity12.key = "Key12";
		entity12.i = 12;
		Entity1 entity13 = new Entity1();
		entity13.key = "Key13";
		entity13.i = 13;
		Key key = Keys.create(Translation.toKind(Entity1.class), "Key11");
		String id = UUID.randomUUID().toString();
		AppEngineGlobalTransaction transaction = new AppEngineGlobalTransaction(id, null, null);
		transaction.logs().add(new Log(1, Log.Operation.PUT, entity11));
		transaction.logs().add(new Log(2, Log.Operation.UPDATE, entity12));
		transaction.logs().add(new Log(3, Log.Operation.DELETE, entity13));
		long start = System.currentTimeMillis();
		List<Entity> entities = Translation.toEntities(transaction, key);
		System.out.println("Translation.toEntities(AppEngineTransaction, Key) [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entities.size(), is(4));
		assertThat(entities.get(0).getKind(), is(Translation.TRANSACTION_KIND));
		Key parent = Keys.create(key, Translation.TRANSACTION_KIND, transaction.id());
		assertThat(entities.get(0).getKey(), is(parent));
		assertThat(entities.get(1).getKind(), is(Translation.LOG_KIND));
		assertThat(entities.get(1).getKey(), is(Keys.create(parent, Translation.LOG_KIND, 1L)));
		assertThat((String) entities.get(1).getProperty(Translation.OPERATION_PROPERTY), is(Log.Operation.PUT.toString()));
		assertThat((String) entities.get(1).getProperty(Translation.CLASS_PROPERTY), is(Entity1.class.getName()));
		assertThat(((List<Blob>) entities.get(1).getProperty(Translation.PROTO_PROPERTY)).size(), is(1));
		assertThat(entities.get(2).getKind(), is(Translation.LOG_KIND));
		assertThat(entities.get(2).getKey(), is(Keys.create(parent, Translation.LOG_KIND, 2L)));
		assertThat((String) entities.get(2).getProperty(Translation.OPERATION_PROPERTY), is(Log.Operation.UPDATE.toString()));
		assertThat((String) entities.get(2).getProperty(Translation.CLASS_PROPERTY), is(Entity1.class.getName()));
		assertThat(((List<Blob>) entities.get(2).getProperty(Translation.PROTO_PROPERTY)).size(), is(1));
		assertThat(entities.get(3).getKind(), is(Translation.LOG_KIND));
		assertThat(entities.get(3).getKey(), is(Keys.create(parent, Translation.LOG_KIND, 3L)));
		assertThat((String) entities.get(3).getProperty(Translation.OPERATION_PROPERTY), is(Log.Operation.DELETE.toString()));
		assertThat((String) entities.get(3).getProperty(Translation.CLASS_PROPERTY), is(Entity1.class.getName()));
		assertThat(((List<Blob>) entities.get(3).getProperty(Translation.PROTO_PROPERTY)).size(), is(1));
	}

	@Test
	public void testToKind() {
		assertThat(Translation.toKind(Entity1.class), is(Entity1.class.getSimpleName()));
		
		try {
			Translation.toKind(Entity6.class);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testToFilterOperator() {
		assertThat(Translation.toFilterOperator(Operator.EQUAL_TO), is(FilterOperator.EQUAL));
		assertThat(Translation.toFilterOperator(Operator.GREATER_THAN), is(FilterOperator.GREATER_THAN));
		assertThat(Translation.toFilterOperator(Operator.GREATER_THAN_OR_EQUAL_TO), is(FilterOperator.GREATER_THAN_OR_EQUAL));
		assertThat(Translation.toFilterOperator(Operator.LESS_THAN), is(FilterOperator.LESS_THAN));
		assertThat(Translation.toFilterOperator(Operator.LESS_THAN_OR_EQUAL_TO), is(FilterOperator.LESS_THAN_OR_EQUAL));
		assertThat(Translation.toFilterOperator(Operator.NOT_EQUAL_TO), is(FilterOperator.NOT_EQUAL));
	}

	@Test
	public void testToSortDirection() {
		assertThat(Translation.toSortDirection(Direction.ASC), is(SortDirection.ASCENDING));
		assertThat(Translation.toSortDirection(Direction.DESC), is(SortDirection.DESCENDING));
	}

}
