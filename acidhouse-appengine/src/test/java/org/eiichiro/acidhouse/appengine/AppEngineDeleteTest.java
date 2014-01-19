package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.List;

import org.eiichiro.acidhouse.Filter;
import org.eiichiro.acidhouse.Transaction;
import org.eiichiro.acidhouse.metamodel.Metamodels;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.DatastoreAttributes.DatastoreType;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class AppEngineDeleteTest {

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
	public void testAppEngineDelete() {}

	@Test
	public void testExecute() {}

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
			System.out.println("AppEngineDelete.filter (Setup) #" + (i + 1) + " [" + (System.currentTimeMillis() - start) + "]");
		}
		
		_Entity3 _Entity3 = Metamodels.metamodel(Entity3.class);
		long start = System.currentTimeMillis();
		Transaction transaction = session.beginTransaction();
		int i = session.delete(_Entity3).filter(_Entity3.key.equalTo("Key1")).execute();
		transaction.commit();
		System.out.println("AppEngineDelete.filter #1 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(i, is(1));
		assertNull(session.get(Entity3.class, "Key1"));
		
		start = System.currentTimeMillis();
		transaction = session.beginTransaction();
		i = session.delete(_Entity3).filter(_Entity3.entity1.i.lessThan(13)).execute();
		transaction.commit();
		System.out.println("AppEngineDelete.filter #2 [" + (System.currentTimeMillis() - start) + "]");
		assertThat(i, is(2));
		List<Entity3> entity3s = session.get(_Entity3).execute();
		assertThat(entity3s.size(), is(2));
		
		try {
			Filter<?> filter = null;
			session.delete(_Entity3).filter(filter);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

}
