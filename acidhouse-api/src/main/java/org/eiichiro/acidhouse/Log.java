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
package org.eiichiro.acidhouse;

/**
 * {@code Log} is a built-in object that represents the datastore operation and 
 * the operation's state. This object is used as a write-ahead-log in 
 * distributed transaction commitment and also used for the state dump when a 
 * transaction is failed.
 * 
 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class Log {

	/**
	 * {@code Operation} is the type of datastore operation.
	 * 
	 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
	 */
	public static enum Operation {
		
		/** Get (Read) operation. */
		GET, 
		
		/** Put (Create) operation. */
		PUT, 
		
		/** Update operation. */
		UPDATE, 
		
		/** Delete operation. */
		DELETE
		
	}
	
	/**
	 * {@code State} is the state of datastore operation.
	 * 
	 * @author <a href="mailto:mail@eiichiro.org">Eiichiro Uchiumi</a>
	 */
	public static enum State {
		
		/** None of the state. */
		NONE, 
		
		/** Log is uncommitted. */
		UNCOMMITTED, 
		
		/** Log is prepared to commit. */
		PREPARED, 
		
		/** Log is committed. */
		COMMITTED
		
	}
	
	private final long sequence;
	
	private final Operation operation;
	
	private State state = State.NONE;
	
	private final Object entity;
	
	/**
	 * Constructs a new {@code Log} instance with the specified operation 
	 * sequence in session, operation type and the entity which this operation 
	 * is applied to.
	 * 
	 * @param sequence The operation sequence in one {@code Session}.
	 * @param operation The operation.
	 * @param entity The entity which this operation is applied to.
	 */
	public Log(long sequence, Operation operation, Object entity) {
		this.sequence = sequence;
		this.operation = operation;
		this.entity = entity;
	}
	
	/**
	 * Returns the operation sequence in session.
	 * 
	 * @return The operation sequence in session.
	 */
	public long sequence() {
		return sequence;
	}

	/**
	 * Returns the operation type.
	 * 
	 * @return The operation type.
	 */
	public Operation operation() {
		return operation;
	}

	/**
	 * Returns the entity which this operation is applied to.
	 * 
	 * @return The entity which this operation is applied to.
	 */
	public Object entity() {
		return entity;
	}

	/**
	 * Sets the specified state into this log.
	 * 
	 * @param state The state of datastore operation.
	 */
	public void state(State state) {
		if (state == null) {
			throw new IllegalArgumentException("'state' must not be [" + state
					+ "]");
		}
		
		this.state = state;
	}

	/**
	 * Returns the state of datastore operation.
	 * 
	 * @return The state of datastore operation.
	 */
	public State state() {
		return state;
	}
	
}
