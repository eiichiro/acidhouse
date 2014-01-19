package org.eiichiro.acidhouse.appengine;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class KeysTest {

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
	public void testCreateStringObject() {}

	@Test
	public void testCreateKeyStringObject() {
		Key key = Keys.create(null, "Entity1", "Key1");
		assertThat(key.getKind(), is("Entity1"));
		assertThat(key.getName(), is("Key1"));
		assertThat(key.getId(), is(0L));
		assertNull(key.getParent());
		key = Keys.create(null, "Entity1", 1L);
		assertThat(key.getKind(), is("Entity1"));
		assertNull(key.getName());
		assertThat(key.getId(), is(1L));
		assertNull(key.getParent());
		key = Keys.create(null, "Entity2", Keys.create(null, "Entity1", "Key1"));
		assertThat(key.getKind(), is("Entity1"));
		assertThat(key.getName(), is("Key1"));
		assertThat(key.getId(), is(0L));
		assertNull(key.getParent());
		key = Keys.create(Keys.create(null, "Entity2", "Key2"), "Entity1", "Key1");
		assertThat(key.getKind(), is("Entity1"));
		assertThat(key.getName(), is("Key1"));
		assertThat(key.getId(), is(0L));
		assertThat(key.getParent().getKind(), is("Entity2"));
		assertThat(key.getParent().getName(), is("Key2"));
		assertThat(key.getParent().getId(), is(0L));
		assertNull(key.getParent().getParent());
		
		try {
			Keys.create(null, "Entity1", new Object());
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testAncestor() {
		Key key = Keys.create(null, "Entity1", "Key1");
		assertSame(key, Keys.ancestor(key));
		key = Keys.create(Keys.create(Keys.create(null, "Entity3", "Key3"), "Entity2", "Key2"), "Entity1", "Key1");
		assertThat(Keys.ancestor(key).getKind(), is("Entity3"));
		assertThat(Keys.ancestor(key).getName(), is("Key3"));
		assertThat(Keys.ancestor(key).getId(), is(0L));
	}

	@Test
	public void testHierarchy() {
		Key key = Keys.create(null, "Entity1", "Key1");
		List<Key> hierarchy = Keys.hierarchy(key);
		assertThat(hierarchy.size(), is(1));
		assertSame(key, hierarchy.get(0));
		key = Keys.create(Keys.create(Keys.create(null, "Entity3", "Key3"), "Entity2", "Key2"), "Entity1", "Key1");
		hierarchy = Keys.hierarchy(key);
		assertThat(hierarchy.size(), is(3));
		assertThat(hierarchy.get(0).getKind(), is("Entity3"));
		assertThat(hierarchy.get(0).getName(), is("Key3"));
		assertThat(hierarchy.get(0).getId(), is(0L));
		assertThat(hierarchy.get(1).getKind(), is("Entity2"));
		assertThat(hierarchy.get(1).getName(), is("Key2"));
		assertThat(hierarchy.get(1).getId(), is(0L));
		assertThat(hierarchy.get(2).getKind(), is("Entity1"));
		assertThat(hierarchy.get(2).getName(), is("Key1"));
		assertThat(hierarchy.get(2).getId(), is(0L));
	}

}
