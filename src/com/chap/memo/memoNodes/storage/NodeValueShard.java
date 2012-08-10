package com.chap.memo.memoNodes.storage;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.chap.memo.memoNodes.model.NodeValue;
import com.eaio.uuid.UUID;
import com.google.common.collect.ArrayListMultimap;

public final class NodeValueShard extends MemoStorable {
	private static final long serialVersionUID = 7295820980658238258L;
	public static final int SHARDSIZE = 30000;
	long oldest = 0;
	long newest = 0;
	int currentSize = 0;
	
	final ArrayListMultimap<UUID,NodeValue> nodes = ArrayListMultimap.create();
	
	
	public ArrayListMultimap<UUID, NodeValue> getNodes() {
		return nodes;
	}
	public int getCurrentSize() {
		return currentSize;
	}
	public void store(NodeValue nodeVal) {
		synchronized (nodes) {
			nodes.put(nodeVal.getId(), nodeVal);
		}
		if (newest == 0 || nodeVal.getTimestamp_long() > newest)
			newest = nodeVal.getTimestamp_long();
		if (oldest == 0 || nodeVal.getTimestamp_long() < oldest)
			oldest = nodeVal.getTimestamp_long();
		currentSize++;
	}

	public List<NodeValue> findAll(UUID id) {
		return nodes.get(id);
	}

	public NodeValue find(UUID id) {
		return findBefore(id, System.currentTimeMillis());
	}

	public NodeValue findBefore(UUID id, Date timestamp) {
		return findBefore(id, timestamp.getTime());
	}

	public NodeValue findBefore(UUID id, long timestamp_long) {
		if (timestamp_long < oldest)
			return null; // shortcut, will probably not be used...

		List<NodeValue> res = nodes.get(id);
		if (res != null && !res.isEmpty()) {
			//Reverse direction might be somewhat faster
			Collections.sort(res);
			NodeValue result = null;
			Iterator<NodeValue> iter = res.iterator();
			while (iter.hasNext()) {
				NodeValue next = iter.next();
				if (next.getTimestamp_long() <= timestamp_long) {
					if (result == null
							|| next.getTimestamp_long() >= result
									.getTimestamp_long()) {
						result = next;
					}
				}
			}
			return result;
		}
		return null;
	}

	public long getOldest() {
		return oldest;
	}

	public long getNewest() {
		return newest;
	}

}
