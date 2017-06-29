package edu.gslis.utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class KeyValuePairs {
	private List<KeyValuePair> tuples;
	
	public KeyValuePairs() {
		tuples = new LinkedList<KeyValuePair>();
	}
	public KeyValuePairs(List<KeyValuePair> tuples) {
		this.tuples = tuples;
	}
	
	
	public void add(KeyValuePair tuple) {
		tuples.add(tuple);
	}
	public void remove(int i) throws Exception {
		if(tuples.size() <= i)
			throw new Exception("can't remove " + i + "th KeyValuePair!");
		tuples.remove(i);
	}
	
	public void sort(boolean decreasing) {
		ScorableComparator comparator = new ScorableComparator(decreasing);
		Collections.sort(tuples, comparator);
	}
	
	public KeyValuePair get(int i) throws Exception {
		if(tuples.size() <= i)
			throw new Exception("can't remove " + i + "th KeyValuePair!");
		return tuples.get(i);
	}
	public Iterator<KeyValuePair> iterator() {
		return tuples.iterator();
	}
	public int size() {
		return tuples.size();
	}
}
