package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.eiichiro.acidhouse.Aggregation;
import org.eiichiro.acidhouse.EntityExistsException;
import org.eiichiro.acidhouse.metamodel.Metamodel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.DatastoreAttributes.DatastoreType;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class AppEngineDatastoreSessionTest {

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
	public void testAppEngineDatastoreSession() {}

	@Test
	public void testBeginTransaction() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		AppEngineTransaction transaction = session.beginTransaction();
		assertThat(transaction, is(AppEngineTransaction.class));
		String id = transaction.id();
		
		try {
			session.beginTransaction();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		transaction.commit();
		assertThat(session.beginTransaction().id(), not(id));
	}

	@Test
	public void testClose() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		session.close();
		
		AppEngineTransaction transaction = session.beginTransaction();
		Entity1 entity = new Entity1();
		entity.key = "Key0";
		entity.i = 0;
		session.put(entity);
		transaction.commit();
		session.beginTransaction();
		entity = session.get(Entity1.class, "Key0");
		entity.i = 10;
		session.update(entity);
		session.close();
		assertThat(session.get(Entity1.class, "Key0").i, is(0));
	}

	@Test
	public void testGetClassOfEObject() {
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity1);
		transaction.commit();
		session.close();
		
		session = new AppEngineDatastoreSession();
		
		try {
			session.get(null, "Key1");
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		try {
			session.get(Entity1.class, null);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		session.close();
		
		session = new AppEngineDatastoreSession();
		long start = System.currentTimeMillis();
		entity1 = session.get(Entity1.class, "Key1");
		System.out.println("AppEngineDatastoreSession.get #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1.key, is("Key1"));
		assertThat(entity1.i, is(1));
		session.close();
		
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity1 = session.get(Entity1.class, "Key1");
		System.out.println("AppEngineDatastoreSession.get #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1.key, is("Key1"));
		assertThat(entity1.i, is(1));
		session.close();
		
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		entity1 = session.get(Entity1.class, "Key11");
		System.out.println("AppEngineDatastoreSession.get #3 [" + (System.currentTimeMillis() - start) + "]");
		assertNull(entity1);
		session.close();
	}

	@Test
	public void testPut() {
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.MASTER_SLAVE);
		
		try {
			session.put(entity1);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		try {
			session.put(null);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		session.close();
		
		// Single entity.
		session = new AppEngineDatastoreSession();
		long start = System.currentTimeMillis();
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity1);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.put #1 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		entity1 = session.get(Entity1.class, "Key1");
		System.out.println("AppEngineDatastoreSession.put #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1.key, is("Key1"));
		assertThat(entity1.i, is(1));
		session.close();
		
		session = new AppEngineDatastoreSession();
		
		try {
			start = System.currentTimeMillis();
			transaction = session.beginTransaction();
			session.put(entity1);
			fail();
		} catch (EntityExistsException e) {
			e.printStackTrace();
		}
		
		session.close();
		
		// Multiple entities.
		Entity1 entity12 = new Entity1();
		entity12.key = "Key12";
		entity12.i = 12;
		Entity1 entity13 = new Entity1();
		entity13.key = "Key13";
		entity13.i = 13;
		Entity1 entity14 = new Entity1();
		entity14.key = "Key14";
		entity14.i = 14;
		session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity12);
		session.put(entity13);
		session.put(entity14);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.put #3 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		entity12 = session.get(Entity1.class, "Key12");
		entity13 = session.get(Entity1.class, "Key13");
		entity14 = session.get(Entity1.class, "Key14");
		System.out.println("AppEngineDatastoreSession.put #4 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity12.key, is("Key12"));
		assertThat(entity12.i, is(12));
		assertThat(entity13.key, is("Key13"));
		assertThat(entity13.i, is(13));
		assertThat(entity14.key, is("Key14"));
		assertThat(entity14.i, is(14));
		session.close();
	}

	@Test
	public void testUpdateObject() {
		Entity3 entity3 = new Entity3();
		entity3.key = "Key3";
		entity3.i = 3;
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		entity3.entity1 = entity1;
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.MASTER_SLAVE);
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity3);
		transaction.commit();
		
		try {
			session = new AppEngineDatastoreSession();
			entity3 = session.get(Entity3.class, "Key3");
			session.update(entity3);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		try {
			session = new AppEngineDatastoreSession();
			session.beginTransaction();
			session.update(entity3);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		try {
			Object object = null;
			session.update(object);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		// Single entity.
		session = new AppEngineDatastoreSession();
		long start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity3 = session.get(Entity3.class, "Key3");
		entity3.i = 30;
		entity3.entity1.i = 10;
		session.update(entity3);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.update #1 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		entity3 = session.get(Entity3.class, "Key3");
		System.out.println("AppEngineDatastoreSession.update #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity3.key, is("Key3"));
		assertThat(entity3.i, is(30));
		assertThat(entity3.entity1.i, is(10));
		session.close();
		
		// Multiple entities.
		Entity3 entity31 = new Entity3();
		entity31.key = "Key31";
		entity31.i = 31;
		Entity3 entity32 = new Entity3();
		entity32.key = "Key32";
		entity32.i = 32;
		Entity3 entity33 = new Entity3();
		entity33.key = "Key33";
		entity33.i = 33;
		Entity1 entity131 = new Entity1();
		entity131.key = "Key131";
		entity131.i = 131;
		Entity1 entity132 = new Entity1();
		entity132.key = "Key132";
		entity132.i = 132;
		Entity1 entity133 = new Entity1();
		entity133.key = "Key133";
		entity133.i = 133;
		entity31.entity1 = entity131;
		entity32.entity1 = entity132;
		entity33.entity1 = entity133;
		session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity31);
		session.put(entity32);
		session.put(entity33);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.update #3 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity31 = session.get(Entity3.class, "Key31");
		entity32 = session.get(Entity3.class, "Key32");
		entity33 = session.get(Entity3.class, "Key33");
		entity31.i = 310;
		entity32.i = 320;
		entity33.i = 330;
		entity31.entity1.i = 1310;
		entity32.entity1.i = 1320;
		entity33.entity1.i = 1330;
		session.update(entity31);
		session.update(entity32);
		session.update(entity33);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.update #4 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		entity31 = session.get(Entity3.class, "Key31");
		entity32 = session.get(Entity3.class, "Key32");
		entity33 = session.get(Entity3.class, "Key33");
		System.out.println("AppEngineDatastoreSession.update #5 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity31.key, is("Key31"));
		assertThat(entity31.i, is(310));
		assertThat(entity31.entity1.key, is("Key131"));
		assertThat(entity31.entity1.i, is(1310));
		assertThat(entity32.key, is("Key32"));
		assertThat(entity32.i, is(320));
		assertThat(entity32.entity1.key, is("Key132"));
		assertThat(entity32.entity1.i, is(1320));
		assertThat(entity33.key, is("Key33"));
		assertThat(entity33.i, is(330));
		assertThat(entity33.entity1.key, is("Key133"));
		assertThat(entity33.entity1.i, is(1330));
		session.close();
	}

	@Test
	public void testAssertTransactional() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		
		try {
			session.assertTransactional();
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		AppEngineTransaction transaction = session.beginTransaction();
		session.assertTransactional();
		transaction.commit();
		
		try {
			session.assertTransactional();
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDeleteObject() {
		Entity3 entity3 = new Entity3();
		entity3.key = "Key3";
		entity3.i = 3;
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		entity3.entity1 = entity1;
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.MASTER_SLAVE);
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity3);
		transaction.commit();
		
		try {
			session = new AppEngineDatastoreSession();
			entity3 = session.get(Entity3.class, "Key3");
			session.delete(entity3);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		try {
			session = new AppEngineDatastoreSession();
			session.beginTransaction();
			session.delete(entity3);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		try {
			Object object = null;
			session.delete(object);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		// Single entity.
		session = new AppEngineDatastoreSession();
		long start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity3 = session.get(Entity3.class, "Key3");
		entity3.i = 30;
		entity3.entity1.i = 10;
		session.delete(entity3);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.delete #1 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		entity3 = session.get(Entity3.class, "Key3");
		System.out.println("AppEngineDatastoreSession.delete #2 [" + (System.currentTimeMillis() - start) + "]");
		assertNull(entity3);
		session.close();
		
		// Multiple entities.
		Entity3 entity31 = new Entity3();
		entity31.key = "Key31";
		entity31.i = 31;
		Entity3 entity32 = new Entity3();
		entity32.key = "Key32";
		entity32.i = 32;
		Entity3 entity33 = new Entity3();
		entity33.key = "Key33";
		entity33.i = 33;
		Entity1 entity131 = new Entity1();
		entity131.key = "Key131";
		entity131.i = 131;
		Entity1 entity132 = new Entity1();
		entity132.key = "Key132";
		entity132.i = 132;
		Entity1 entity133 = new Entity1();
		entity133.key = "Key133";
		entity133.i = 133;
		entity31.entity1 = entity131;
		entity32.entity1 = entity132;
		entity33.entity1 = entity133;
		session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity31);
		session.put(entity32);
		session.put(entity33);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.delete #3 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		session.datastore().datastoreType(DatastoreType.HIGH_REPLICATION);
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity31 = session.get(Entity3.class, "Key31");
		entity32 = session.get(Entity3.class, "Key32");
		entity33 = session.get(Entity3.class, "Key33");
		entity31.i = 310;
		entity32.i = 320;
		entity33.i = 330;
		entity31.entity1.i = 1310;
		entity32.entity1.i = 1320;
		entity33.entity1.i = 1330;
		session.delete(entity31);
		session.delete(entity32);
		session.delete(entity33);
		transaction.commit();
		System.out.println("AppEngineDatastoreSession.delete #4 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineDatastoreSession();
		start = System.currentTimeMillis();
		entity31 = session.get(Entity3.class, "Key31");
		entity32 = session.get(Entity3.class, "Key32");
		entity33 = session.get(Entity3.class, "Key33");
		System.out.println("AppEngineDatastoreSession.delete #5 [" + (System.currentTimeMillis() - start) + "]");
		assertNull(entity31);
		assertNull(entity32);
		assertNull(entity33);
		session.close();
	}

	@Test
	public void testGetMetamodelOfE() {
		try {
			Metamodel<?> metamodel = null;
			new AppEngineDatastoreSession().get(metamodel);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetAggregationOfR() {
		try {
			Aggregation<?> aggregation = null;
			new AppEngineDatastoreSession().update(aggregation);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testUpdateMetamodelOfE() {
		try {
			Metamodel<?> metamodel = null;
			new AppEngineDatastoreSession().update(metamodel);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDeleteMetamodelOfE() {
		try {
			Metamodel<?> metamodel = null;
			new AppEngineDatastoreSession().delete(metamodel);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIsTransactional() {
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		assertFalse(session.isTransactional());
		AppEngineTransaction transaction = session.beginTransaction();
		assertTrue(session.isTransactional());
		transaction.commit();
		assertFalse(session.isTransactional());
	}

}
