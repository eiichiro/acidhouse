package org.eiichiro.acidhouse.appengine;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

import org.eiichiro.acidhouse.EntityExistsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class AppEngineStrongDatastoreSessionTest {

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
	public void testBeginTransaction() {
		AppEngineStrongDatastoreSession session = new AppEngineStrongDatastoreSession();
		AppEngineTransaction transaction = session.beginTransaction();
		assertThat(transaction, is(AppEngineGlobalTransaction.class));
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
	public void testGetClassOfEObject() {
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		AppEngineStrongDatastoreSession session = new AppEngineStrongDatastoreSession();
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity1);
		transaction.commit();
		session.close();
		
		session = new AppEngineStrongDatastoreSession();
		
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
		
		session = new AppEngineStrongDatastoreSession();
		long start = System.currentTimeMillis();
		entity1 = session.get(Entity1.class, "Key1");
		System.out.println("AppEngineStrongDatastoreSession.get #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1.key, is("Key1"));
		assertThat(entity1.i, is(1));
		session.close();
		
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity1 = session.get(Entity1.class, "Key1");
		System.out.println("AppEngineStrongDatastoreSession.get #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1.key, is("Key1"));
		assertThat(entity1.i, is(1));
		session.close();
		
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity1 = session.get(Entity1.class, "Key11");
		System.out.println("AppEngineStrongDatastoreSession.get #3 [" + (System.currentTimeMillis() - start) + "]");
		assertNull(entity1);
		session.close();
	}

	@Test
	public void testPut() {
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		AppEngineStrongDatastoreSession session = new AppEngineStrongDatastoreSession();
		
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
		session = new AppEngineStrongDatastoreSession();
		long start = System.currentTimeMillis();
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity1);
		transaction.commit();
		System.out.println("AppEngineStrongDatastoreSession.put #1 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity1 = session.get(Entity1.class, "Key1");
		System.out.println("AppEngineStrongDatastoreSession.put #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity1.key, is("Key1"));
		assertThat(entity1.i, is(1));
		session.close();
		
		session = new AppEngineStrongDatastoreSession();
		
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
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity12);
		session.put(entity13);
		session.put(entity14);
		transaction.commit();
		System.out.println("AppEngineStrongDatastoreSession.put #3 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity12 = session.get(Entity1.class, "Key12");
		entity13 = session.get(Entity1.class, "Key13");
		entity14 = session.get(Entity1.class, "Key14");
		System.out.println("AppEngineStrongDatastoreSession.put #4 [" + (System.currentTimeMillis() - start) + "]");
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
		AppEngineStrongDatastoreSession session = new AppEngineStrongDatastoreSession();
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity3);
		transaction.commit();
		
		try {
			session = new AppEngineStrongDatastoreSession();
			entity3 = session.get(Entity3.class, "Key3");
			session.update(entity3);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		try {
			session = new AppEngineStrongDatastoreSession();
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
		session = new AppEngineStrongDatastoreSession();
		long start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity3 = session.get(Entity3.class, "Key3");
		entity3.i = 30;
		entity3.entity1.i = 10;
		session.update(entity3);
		transaction.commit();
		System.out.println("AppEngineStrongDatastoreSession.update #1 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity3 = session.get(Entity3.class, "Key3");
		System.out.println("AppEngineStrongDatastoreSession.update #2 [" + (System.currentTimeMillis() - start) + "]");
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
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity31);
		session.put(entity32);
		session.put(entity33);
		transaction.commit();
		System.out.println("AppEngineStrongDatastoreSession.update #3 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
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
		System.out.println("AppEngineStrongDatastoreSession.update #4 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity31 = session.get(Entity3.class, "Key31");
		entity32 = session.get(Entity3.class, "Key32");
		entity33 = session.get(Entity3.class, "Key33");
		System.out.println("AppEngineStrongDatastoreSession.update #5 [" + (System.currentTimeMillis() - start) + "]");
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
	public void testDeleteObject() {
		Entity3 entity3 = new Entity3();
		entity3.key = "Key3";
		entity3.i = 3;
		Entity1 entity1 = new Entity1();
		entity1.key = "Key1";
		entity1.i = 1;
		entity3.entity1 = entity1;
		AppEngineStrongDatastoreSession session = new AppEngineStrongDatastoreSession();
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity3);
		transaction.commit();
		
		try {
			session = new AppEngineStrongDatastoreSession();
			entity3 = session.get(Entity3.class, "Key3");
			session.delete(entity3);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		try {
			session = new AppEngineStrongDatastoreSession();
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
		session = new AppEngineStrongDatastoreSession();
		long start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		entity3 = session.get(Entity3.class, "Key3");
		entity3.i = 30;
		entity3.entity1.i = 10;
		session.delete(entity3);
		transaction.commit();
		System.out.println("AppEngineStrongDatastoreSession.delete #1 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity3 = session.get(Entity3.class, "Key3");
		System.out.println("AppEngineStrongDatastoreSession.delete #2 [" + (System.currentTimeMillis() - start) + "]");
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
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity31);
		session.put(entity32);
		session.put(entity33);
		transaction.commit();
		System.out.println("AppEngineStrongDatastoreSession.delete #3 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
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
		System.out.println("AppEngineStrongDatastoreSession.delete #4 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity31 = session.get(Entity3.class, "Key31");
		entity32 = session.get(Entity3.class, "Key32");
		entity33 = session.get(Entity3.class, "Key33");
		System.out.println("AppEngineStrongDatastoreSession.delete #5 [" + (System.currentTimeMillis() - start) + "]");
		assertNull(entity31);
		assertNull(entity32);
		assertNull(entity33);
		session.close();
	}

}
