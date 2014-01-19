package org.eiichiro.acidhouse.appengine;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.dev.HighRepJobPolicy;

public class NeverFailHighRepJobPolicy implements HighRepJobPolicy {

	@Override
	public boolean shouldApplyNewJob(Key key) {
		return true;
	}

	@Override
	public boolean shouldRollForwardExistingJob(Key key) {
		return true;
	}

}
