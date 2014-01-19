package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.ConcurrentModificationException;

import org.eiichiro.acidhouse.EntityExistsException;
import org.eiichiro.acidhouse.IndoubtException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.tools.development.ApiProxyLocal;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.apphosting.api.ApiProxy;

public class AppEngineResourceManagerTest {

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
	public void testAppEngineResourceManagerAppEngineDatastoreService() {}

	@Test
	public void testAppEngineResourceManagerAppEngineDatastoreServiceTransactionAppEngineGlobalTransaction() {}

	@Test
	public void testGet() throws InterruptedException {
		// Success.
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
		AppEngineStrongDatastoreSession session = new AppEngineStrongDatastoreSession();
		long start = System.currentTimeMillis();
		AppEngineTransaction transaction = session.beginTransaction();
		session.put(entity31);
		session.put(entity32);
		session.put(entity33);
		transaction.commit();
		System.out.println("AppEngineResourceManager.get #1 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		entity31 = session.get(Entity3.class, "Key31");
		entity32 = session.get(Entity3.class, "Key32");
		entity33 = session.get(Entity3.class, "Key33");
		System.out.println("AppEngineResourceManager.get #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity31.key, is("Key31"));
		assertThat(entity31.i, is(31));
		assertThat(entity31.entity1.key, is("Key131"));
		assertThat(entity31.entity1.i, is(131));
		assertThat(entity32.key, is("Key32"));
		assertThat(entity32.i, is(32));
		assertThat(entity32.entity1.key, is("Key132"));
		assertThat(entity32.entity1.i, is(132));
		assertThat(entity33.key, is("Key33"));
		assertThat(entity33.i, is(33));
		assertThat(entity33.entity1.key, is("Key133"));
		assertThat(entity33.entity1.i, is(133));
		session.close();
		
		// Fault.
		ApiProxyLocal delegate = (ApiProxyLocal) ApiProxy.getDelegate();
		FaultDelegate indoubt = new FaultDelegate(delegate, 1);
		ApiProxy.setDelegate(indoubt);
		Entity3 entity34 = new Entity3();
		entity34.key = "Key34";
		entity34.i = 34;
		Entity3 entity35 = new Entity3();
		entity35.key = "Key35";
		entity35.i = 35;
		Entity3 entity36 = new Entity3();
		entity36.key = "Key36";
		entity36.i = 36;
		Entity1 entity134 = new Entity1();
		entity134.key = "Key134";
		entity134.i = 134;
		Entity1 entity135 = new Entity1();
		entity135.key = "Key135";
		entity135.i = 135;
		Entity1 entity136 = new Entity1();
		entity136.key = "Key136";
		entity136.i = 136;
		entity34.entity1 = entity134;
		entity35.entity1 = entity135;
		entity36.entity1 = entity136;
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity34);
		session.put(entity35);
		session.put(entity36);
		
		try {
			transaction.commit();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			transaction.rollback();
		}
		
		System.out.println("AppEngineResourceManager.get #3 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		
		try {
			entity34 = session.get(Entity3.class, "Key34");
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			assertThat(e, is(ConcurrentModificationException.class));
		}
		
		ApiProxy.setDelegate(delegate);
		Thread.sleep((long) session.datastore().deadline());
		start = System.currentTimeMillis();
		entity34 = session.get(Entity3.class, "Key34");
		entity35 = session.get(Entity3.class, "Key35");
		entity36 = session.get(Entity3.class, "Key36");
		System.out.println("AppEngineResourceManager.get #4 [" + (System.currentTimeMillis() - start) + "]");
		assertNull(entity34);
		assertNull(entity35);
		assertNull(entity36);
		session.close();
		
		// Consistent read.
		indoubt = new FaultDelegate(delegate, 4);
		ApiProxy.setDelegate(indoubt);
		Entity3 entity37 = new Entity3();
		entity37.key = "Key37";
		entity37.i = 37;
		Entity3 entity38 = new Entity3();
		entity38.key = "Key38";
		entity38.i = 38;
		Entity3 entity39 = new Entity3();
		entity39.key = "Key39";
		entity39.i = 39;
		Entity1 entity137 = new Entity1();
		entity137.key = "Key137";
		entity137.i = 137;
		Entity1 entity138 = new Entity1();
		entity138.key = "Key138";
		entity138.i = 138;
		Entity1 entity139 = new Entity1();
		entity139.key = "Key139";
		entity139.i = 139;
		entity37.entity1 = entity137;
		entity38.entity1 = entity138;
		entity39.entity1 = entity139;
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity37);
		session.put(entity38);
		session.put(entity39);
		transaction.commit();
		System.out.println("AppEngineResourceManager.get #5 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		ApiProxy.setDelegate(delegate);
		Thread.sleep((long) session.datastore().deadline());
		start = System.currentTimeMillis();
		entity37 = session.get(Entity3.class, "Key37");
		entity38 = session.get(Entity3.class, "Key38");
		entity39 = session.get(Entity3.class, "Key39");
		System.out.println("AppEngineResourceManager.get #6 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(entity37.key, is("Key37"));
		assertThat(entity37.i, is(37));
		assertThat(entity37.entity1.key, is("Key137"));
		assertThat(entity37.entity1.i, is(137));
		assertThat(entity38.key, is("Key38"));
		assertThat(entity38.i, is(38));
		assertThat(entity38.entity1.key, is("Key138"));
		assertThat(entity38.entity1.i, is(138));
		assertThat(entity39.key, is("Key39"));
		assertThat(entity39.i, is(39));
		assertThat(entity39.entity1.key, is("Key139"));
		assertThat(entity39.entity1.i, is(139));
		session.close();
		
		// In-doubt.
		indoubt = new FaultDelegate(delegate, 4);
		ApiProxy.setDelegate(indoubt);
		Entity3 entity310 = new Entity3();
		entity310.key = "Key310";
		entity310.i = 310;
		Entity3 entity311 = new Entity3();
		entity311.key = "Key311";
		entity311.i = 311;
		Entity3 entity312 = new Entity3();
		entity312.key = "Key312";
		entity312.i = 312;
		Entity1 entity1310 = new Entity1();
		entity1310.key = "Key1310";
		entity1310.i = 1310;
		Entity1 entity1311 = new Entity1();
		entity1311.key = "Key1311";
		entity1311.i = 1311;
		Entity1 entity1312 = new Entity1();
		entity1312.key = "Key1312";
		entity1312.i = 1312;
		entity310.entity1 = entity1310;
		entity311.entity1 = entity1311;
		entity312.entity1 = entity1312;
		session = new AppEngineStrongDatastoreSession();
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		session.put(entity310);
		session.put(entity311);
		session.put(entity312);
		transaction.commit();
		System.out.println("AppEngineResourceManager.get #7 [" + (System.currentTimeMillis() - start) + "]");
		session.close();
		session = new AppEngineStrongDatastoreSession();
		Thread.sleep((long) session.datastore().deadline());
		indoubt = new FaultDelegate(delegate, 1);
		ApiProxy.setDelegate(indoubt);
		start = System.currentTimeMillis();
		entity310 = session.get(Entity3.class, "Key310");
		
		try {
			entity311 = session.get(Entity3.class, "Key311");
			fail();
		} catch (IndoubtException e) {
			e.printStackTrace();
		}
		
		System.out.println("AppEngineResourceManager.get #8 [" + (System.currentTimeMillis() - start) + "]");
		ApiProxy.setDelegate(delegate);
		session.close();
	}

	@Test
	public void testPut() {
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		Transaction transaction = datastore.beginTransaction();
		AppEngineResourceManager manager = new AppEngineResourceManager(new AppEngineDatastoreService(datastore), transaction, null);
		
		try {
			manager.put(null);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		Entity1 entity = new Entity1();
		entity.key = "Key1";
		manager.put(entity);
		
		try {
			manager.put(entity);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		transaction.rollback();
		
		AppEngineDatastoreSession session = new AppEngineDatastoreSession();
		AppEngineTransaction appEngineTransaction = session.beginTransaction();
		session.put(entity);
		appEngineTransaction.commit();
		transaction = datastore.beginTransaction();
		manager = new AppEngineResourceManager(new AppEngineDatastoreService(datastore), transaction, null);
		
		try {
			manager.put(entity);
			fail();
		} catch (EntityExistsException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testUpdate() {
		AppEngineResourceManager manager = new AppEngineResourceManager(new AppEngineDatastoreService(DatastoreServiceFactory.getDatastoreService()));
		
		try {
			manager.update(null);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		Entity1 entity = new Entity1();
		entity.key = "Key1";
		
		try {
			manager.update(entity);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDelete() {
		AppEngineResourceManager manager = new AppEngineResourceManager(new AppEngineDatastoreService(DatastoreServiceFactory.getDatastoreService()));
		
		try {
			manager.delete(null);
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		Entity1 entity = new Entity1();
		entity.key = "Key1";
		
		try {
			manager.delete(entity);
			fail();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPrepare() {
		// AppEngineStrongDatastoreSessionTest
	}

	@Test
	public void testCommit() {
		// AppEngineStrongDatastoreSessionTest
	}

	@Test
	public void testEntity() {
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		Transaction transaction = datastore.beginTransaction();
		AppEngineResourceManager manager = new AppEngineResourceManager(new AppEngineDatastoreService(datastore), transaction, null);
		assertNull(manager.entity());
		Entity1 entity = new Entity1();
		entity.key = "Key1";
		manager.put(entity);
		assertThat(manager.entity(), is((Object) entity));
	}

	@Test
	public void testTransaction() {
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		AppEngineResourceManager manager = new AppEngineResourceManager(new AppEngineDatastoreService(datastore));
		assertNull(manager.transaction());
		Transaction transaction = datastore.beginTransaction();
		manager = new AppEngineResourceManager(new AppEngineDatastoreService(datastore), transaction, null);
		assertThat(manager.transaction(), is(transaction));
	}

}
