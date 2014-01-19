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

import java.util.List;

/**
 * {@code IndoubtException} is thrown by {@code Session} when it 
 * detects the data is consistency broken (The situation that there are 
 * committed operations and uncommitted operations in one transaction). If you 
 * get this exception, you have to recover your data consistency immediately 
 * from the transaction log dump.
 * 
 * @author <a href="mailto:eiichiro@eiichiro.org">Eiichiro Uchiumi</a>
 */
public class IndoubtException extends RuntimeException {

	private static final long serialVersionUID = 1076359462199207688L;
	
	private final List<Log> logs;

	/**
	 * Constructs a new {@code IndoubtException} instance with the specified 
	 * cause, transaction id and logs.
	 * 
	 * @param exception The cause exception.
	 * @param id The current datastore transaction id.
	 * @param logs The logs operated in the current datastore transaction.
	 */
	public IndoubtException(Exception exception, String id, List<Log> logs) {
		super(dump(id, logs), exception);
		this.logs = logs;
	}
	
	private static String dump(String id, List<Log> logs) {
		StringBuilder builder = new StringBuilder();
		builder.append("Transaction [" + id + "] is in-doubt; Dump: \n");
		
		for (int i = 0; i < logs.size(); i++) {
			Log log = logs.get(i);
			builder.append("\tOperation " + (i + 1) + " ["
					+ log.operation() + "] -> ["
					+ log.state()
					+ "]: Entity [" + Entities.keyValue(log.entity())
					+ "]\n");
		}
		
		return builder.toString();
	}

	public List<Log> logs() {
		return logs;
	}
	
}
