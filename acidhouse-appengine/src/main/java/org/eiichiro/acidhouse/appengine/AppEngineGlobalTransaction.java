/*
 * Copyright (C) 2011 Eiichiro Uchiumi. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eiichiro.acidhouse.appengine;

import java.util.logging.Logger;

import org.eiichiro.acidhouse.IndoubtException;

/**
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineGlobalTransaction extends AppEngineTransaction {

	private final Logger logger = Logger.getLogger(getClass().getName());
	
	private final AppEngineStrongDatastoreSession session;
	
	private final AppEngineCoordinator coordinator;
	
	/**
	 * Constructs a new {@code AppEngineGlobalTransaction} with the specified 
	 * {@code AppEngineStrongDatastoreSession} and {@code AppEngineCoordinator}.
	 * 
	 * @param session {@code AppEngineStrongDatastoreSession} that starts this 
	 * transaction.
	 */
	public AppEngineGlobalTransaction(AppEngineStrongDatastoreSession session) {
		super(session);
		this.session = session;
		this.coordinator = new AppEngineCoordinator(this, session.datastore());
	}
	
	/**
	 * Constructs a new {@code AppEngineGlobalTransaction} with the specified 
	 * transaction id, {@code AppEngineStrongDatastoreSession} and 
	 * {@code AppEngineCoordinator}.
	 * 
	 * @param id The transaction id.
	 * @param session {@code AppEngineStrongDatastoreSession} that starts this 
	 * transaction.
	 * @param coordinator {@code AppEngineCoordinator} that starts this 
	 * transaction.
	 */
	public AppEngineGlobalTransaction(String id, 
			AppEngineStrongDatastoreSession session,
			AppEngineCoordinator coordinator) {
		super(id, session, null);
		this.session = session;
		this.coordinator = coordinator;
	}

	/** Commits current transaction with {@code AppEngineCoordinator}. */
	@Override
	public void commit() throws IndoubtException {
		session.transaction.remove();
		coordinator.commit();
		logger.fine("Transaction [" + id() + "] committed");
	}

	/** Rolls back current transaction with {@code AppEngineCoordinator}. */
	@Override
	public void rollback() {
		session.transaction.remove();
		coordinator.rollback();
	}

	public AppEngineCoordinator coordinator() {
		return coordinator;
	}
	
}
