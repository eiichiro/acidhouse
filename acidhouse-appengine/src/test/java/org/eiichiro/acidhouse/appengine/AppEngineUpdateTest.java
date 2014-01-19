package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.List;

import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.Transaction;
import org.eiichiro.acidhouse.Update;
import org.eiichiro.acidhouse.metamodel.Metamodels;
import org.eiichiro.acidhouse.metamodel.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.DatastoreAttributes.DatastoreType;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class AppEngineUpdateTest {

	private LocalServiceTestHelper helper = new LocalServiceTestHelper(
			new LocalDatastoreServiceTestConfig().setAlternateHighRepJobPolicyClass(NeverFailHighRepJobPolicy.class));
	
	@Before
	public void setUp() throws Exception {
		helper.setUp();
	}

	@After
	public void tearDown() throws Exception {
		helper.tearDown();
	}

	@Test
	public void testAppEngineUpdate() {}

	@Test
	public void testExecute() {
		try {
			AppEngineDatastoreSession session = new AppEngineDatastoreSession();
			_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
			session.beginTransaction();
			session.update(_Entity3).execute();
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSet() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		
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
			System.out.println("AppEngineUpdate.set (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		long start = System.currentTimeMillis();
		Transaction transaction = session.beginTransaction();
		int i = session.update(_Entity3)
				.set(_Entity3.i, 100)
				.set(_Entity3.entity1.i, 200)
				.filter(_Entity3.key.equalTo("Key1")).execute();
		transaction.commit();
		System.out.println("AppEngineUpdate.set #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(i, is(1));
		Entity3 entity3 = session.get(Entity3.class, "Key1");
		assertThat(entity3.i, is(100));
		assertThat(entity3.entity1.i, is(200));
		
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		i = session.update(_Entity3).set(_Entity3.i, 200)
				.filter(_Entity3.entity1.i.lessThan(13)).execute();
		transaction.commit();
		System.out.println("AppEngineUpdate.set #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(i, is(2));
		List<Entity3> entity3s = session.get(_Entity3).execute();
		assertThat(entity3s.size(), is(5));
		assertThat(entity3s.get(0).i, is(200));
		assertThat(entity3s.get(1).i, is(100));
		assertThat(entity3s.get(2).i, is(200));
		assertThat(entity3s.get(3).i, is(3));
		assertThat(entity3s.get(4).i, is(4));
		
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		i = session.update(_Entity3)
				.set(_Entity3.i, new Update.Modification<Integer>() {

					@Override
					public Integer apply(Integer integer) {
						return integer + 100;
					}
					
				})
				.set(_Entity3.entity1.i, new Update.Modification<Integer>() {

					@Override
					public Integer apply(Integer integer) {
						return integer + 10;
					}
					
				})
				.filter(_Entity3.entity1.i.lessThan(13)).execute();
		transaction.commit();
		System.out.println("AppEngineUpdate.set #3 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(i, is(2));
		entity3s = session.get(_Entity3).execute();
		assertThat(entity3s.size(), is(5));
		assertThat(entity3s.get(0).i, is(300));
		assertThat(entity3s.get(0).entity1.i, is(20));
		assertThat(entity3s.get(1).i, is(100));
		assertThat(entity3s.get(1).entity1.i, is(200));
		assertThat(entity3s.get(2).i, is(300));
		assertThat(entity3s.get(2).entity1.i, is(22));
		assertThat(entity3s.get(3).i, is(3));
		assertThat(entity3s.get(3).entity1.i, is(13));
		assertThat(entity3s.get(4).i, is(4));
		assertThat(entity3s.get(4).entity1.i, is(14));
		
		try {
			Property<?, Object> property = null;
			session.update(_Entity3).set(property, new Object());
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testFilter() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		
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
			System.out.println("AppEngineUpdate.filter (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		long start = System.currentTimeMillis();
		Transaction transaction = session.beginTransaction();
		int i = session.update(_Entity3).set(_Entity3.i, 100)
				.filter(_Entity3.key.equalTo("Key1")).execute();
		transaction.commit();
		System.out.println("AppEngineUpdate.filter #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(i, is(1));
		assertThat(session.get(Entity3.class, "Key1").i, is(100));
		
		transaction = session.beginTransaction();
		i = session.update(_Entity3).set(_Entity3.i, 200)
				.filter(_Entity3.entity1.i.lessThan(13)).execute();
		transaction.commit();
		assertThat(i, is(3));
		List<Entity3> entity3s = session.get(_Entity3).execute();
		assertThat(entity3s.size(), is(5));
		assertThat(entity3s.get(0).i, is(200));
		assertThat(entity3s.get(1).i, is(200));
		assertThat(entity3s.get(2).i, is(200));
		assertThat(entity3s.get(3).i, is(3));
		assertThat(entity3s.get(4).i, is(4));
		
		try {
			Filter<?> filter = null;
			session.update(_Entity3).filter(filter);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

}
