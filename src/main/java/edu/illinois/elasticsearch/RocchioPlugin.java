package edu.illinois.elasticsearch;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

public class RocchioPlugin extends Plugin {

	public List<Class<? extends RestHandler>> getRestHandlers() {
	    return Collections.singletonList(RocchioRestAction.class);
	}
}
