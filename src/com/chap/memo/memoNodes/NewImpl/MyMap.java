package com.chap.memo.memoNodes.NewImpl;

import java.util.LinkedHashMap;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class MyMap<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = 1L;
	private int capacity=10;

	MyMap(int capacity, float loadFactor, boolean order) {
		super(capacity, loadFactor, order);
		this.capacity=capacity;
	}

	protected boolean removeEldestEntry(Map.Entry eldest) {
		synchronized (this) {
			if (size() > capacity) {
				this.remove(eldest.getKey());
			}
		}
		return false;
	}
}
