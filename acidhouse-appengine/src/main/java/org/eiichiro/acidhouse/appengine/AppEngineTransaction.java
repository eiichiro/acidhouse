/*
 * Copyright (C) 2011-2012 Eiichiro Uchiumi. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.eiichiro.acidhouse.IndoubtException;
import org.eiichiro.acidhouse.Log;
import org.eiichiro.acidhouse.Log.Operation;
import org.eiichiro.acidhouse.Log.State;
import org.eiichiro.reverb.lang.UncheckedException;

import com.google.appengine.api.datastore.Transaction;

/**
 * {@code AppEngineTransaction} is a Google App Engine Low-level Datastore API 
 * based implementation of {@code org.eiichiro.acidhouse.Transaction}.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class AppEngineTransaction implements org.eiichiro.acidhouse.Transaction {

	private final Logger logger = Logger.getLogger(getClass().getName());
	
	private final String id;
	
	private final List<Log> logs = new ArrayList<Log>();
	
	private final AppEngineDatastoreSession session;
	
	private final Transaction transaction;
	
	/**
	 * Constructs a new {@code AppEngineTransaction} instance with random UUID 
	 * based transaction id and the specified {@code AppEngineDatastoreSession} 
	 * instance.
	 * 
	 * @param session {@code AppEngineDatastoreSession} instance.
	 */
	public AppEngineTransaction(AppEngineDatastoreSession session) {
		id = UUID.randomUUID().toString();
		this.session = session;
		transaction = session.datastore().beginTransaction();
	}
	
	/**
	 * Constructs a new {@code AppEngineTransaction} instance with the specified 
	 * transaction id and the specified {@code AppEngineDatastoreSession} 
	 * instance.
	 * 
	 * @param id The transaction id.
	 * @param session {@code AppEngineDatastoreSession} instance.
	 * @param transaction App Engine Low-level {@code Transaction}.
	 */
	public AppEngineTransaction(String id, AppEngineDatastoreSession session, 
			Transaction transaction) {
		this.id = id;
		this.session = session;
		this.transaction = transaction;
	}
	
	@Override
	public String id() {
		return id;
	}

	/** Commits this transaction with loose commitment protocol. */
	@Override
	public void commit() throws IndoubtException {
		session.transaction.remove();
		
		if (logs.size() == 0) {
			return;
		}
		
		try {
			for (Log log : logs) {
				if (log.operation() != Operation.GET) {
					if (transaction.isActive()) {
						transaction.commit();
					}
					
					log.state(State.COMMITTED);
				}
			}

		} catch (Exception e) {
			throw new UncheckedException(e);
		}
		
		logger.fine("Transaction [" + id + "] committed");
	}

	/** Rolls back this transaction. */
	@Override
	public void rollback() {
		session.transaction.remove();
		
		if (transaction.isActive()) {
			transaction.rollback();
		}
	}

	/**
	 * @return the logs
	 */
	public List<Log> logs() {
		return logs;
	}
	
	Transaction transaction() {
		return transaction;
	}

}
