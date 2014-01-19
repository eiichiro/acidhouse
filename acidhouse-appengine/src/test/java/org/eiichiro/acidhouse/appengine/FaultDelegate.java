package org.eiichiro.acidhouse.appengine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.appengine.tools.development.ApiProxyLocal;
import com.google.appengine.tools.development.Clock;
import com.google.appengine.tools.development.LocalRpcService;
import com.google.apphosting.api.ApiProxy.ApiConfig;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.apphosting.api.ApiProxy.Environment;
import com.google.apphosting.api.ApiProxy.LogRecord;
import com.google.apphosting.api.DatastorePb;

public class FaultDelegate implements ApiProxyLocal {

	private final ApiProxyLocal delegate;
	
	private final int limit;
	
	private int count = 0;
	
	public FaultDelegate(ApiProxyLocal delegate, int limit) {
		this.delegate = delegate;
		this.limit = limit;
	}
	
	@Override
	public void flushLogs(Environment environment) {
		delegate.flushLogs(environment);
	}

	@Override
	public List<Thread> getRequestThreads(Environment environment) {
		return delegate.getRequestThreads(environment);
	}

	@Override
	public void log(Environment environment, LogRecord record) {
		delegate.log(environment, record);
	}

	@Override
	public Future<byte[]> makeAsyncCall(Environment environment, String pkg,
			String method, byte[] request, ApiConfig config) {
		System.out.println(method);
		
		if (method.equals("Get")) {
			DatastorePb.GetRequest getRequest = new DatastorePb.GetRequest();
			getRequest.mergeFrom(request);
			System.out.println(getRequest);
		} else if (method.equals("Put")) {
			DatastorePb.PutRequest putRequest = new DatastorePb.PutRequest();
			putRequest.mergeFrom(request);
			System.out.println(putRequest);
		} else if (method.equals("Delete")) {
			DatastorePb.DeleteRequest deleteRequest = new DatastorePb.DeleteRequest();
			deleteRequest.mergeFrom(request);
			System.out.println(deleteRequest);
		} else if (method.equals("RunQuery")) {
			DatastorePb.Query query = new DatastorePb.Query();
			query.mergeFrom(request);
			System.out.println(query);
		} else if (method.equals("Commit")) {
			if (count + 1 > limit) {
				throw new RuntimeException("Commit count [" + (count + 1)
						+ "] exceeds the given limit [" + limit + "]");
			}
			
			DatastorePb.Transaction transaction = new DatastorePb.Transaction();
			transaction.mergeFrom(request);
			count++;
			System.out.println(transaction);
		}
		
		return delegate.makeAsyncCall(environment, pkg, method, request, config);
	}

	@Override
	public byte[] makeSyncCall(Environment environment, String pkg, String method,
			byte[] request) throws ApiProxyException {
		System.out.println(method);
		
		if (method.equals("Get")) {
			DatastorePb.GetRequest getRequest = new DatastorePb.GetRequest();
			getRequest.mergeFrom(request);
			System.out.println(getRequest);
		} else if (method.equals("Put")) {
			DatastorePb.PutRequest putRequest = new DatastorePb.PutRequest();
			putRequest.mergeFrom(request);
			System.out.println(putRequest);
		} else if (method.equals("Delete")) {
			DatastorePb.DeleteRequest deleteRequest = new DatastorePb.DeleteRequest();
			deleteRequest.mergeFrom(request);
			System.out.println(deleteRequest);
		} else if (method.equals("RunQuery")) {
			DatastorePb.Query query = new DatastorePb.Query();
			query.mergeFrom(request);
			System.out.println(query);
		} else if (method.equals("Commit")) {
			if (count + 1 > limit) {
				throw new RuntimeException("Commit count [" + (count + 1)
						+ "] exceeds the given limit [" + limit + "]");
			}
			
			DatastorePb.Transaction transaction = new DatastorePb.Transaction();
			transaction.mergeFrom(request);
			count++;
			System.out.println(transaction);
		}
		
		return delegate.makeSyncCall(environment, pkg, method, request);
	}

	@Override
	public void appendProperties(Map<String, String> properties) {
		delegate.appendProperties(properties);
	}

	@Override
	public Clock getClock() {
		return delegate.getClock();
	}

	@Override
	public LocalRpcService getService(String name) {
		return delegate.getService(name);
	}

	@Override
	public void setClock(Clock clock) {
		delegate.setClock(clock);
	}

	@Override
	public void setProperties(Map<String, String> properties) {
		delegate.setProperties(properties);
	}

	@Override
	public void setProperty(String key, String value) {
		delegate.setProperty(key, value);
	}

	@Override
	public void stop() {
		delegate.stop();
	}

}
