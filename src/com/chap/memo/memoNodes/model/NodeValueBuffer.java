package com.chap.memo.memoNodes.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.storage.NodeValueIndex;
import com.chap.memo.memoNodes.storage.NodeValueShard;
import com.eaio.uuid.UUID;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

public class NodeValueBuffer {
	public static final int STORESIZE = 50000;
	public static final int COMPRESSION_RATIO = 8;

	MemoReadBus ReadBus;

	public static final ArrayListMultimap<Long, NodeValue> template = ArrayListMultimap
			.create();
	public transient ListMultimap<Long, NodeValue> nodes = Multimaps
			.synchronizedListMultimap(ArrayListMultimap.create(template));
	long oldest = 0;
	long newest = 0;
	long size = 0;
	
	public void store(NodeValue nodeVal) {
		synchronized (this) {
			nodes.put(nodeVal.getId().time, nodeVal);
			if (newest == 0 || nodeVal.getTimestamp_long() > newest)
				newest = nodeVal.getTimestamp_long();
			if (oldest == 0 || nodeVal.getTimestamp_long() < oldest)
				oldest = nodeVal.getTimestamp_long();
			if ((size+=(nodeVal.getValue().length+48)) >= STORESIZE*COMPRESSION_RATIO) {
//				System.out.println("Size grown to:"+size+", flushing!");
				flush();
			}

		}
	}

	public void flush() {
		ArrayList<NodeValueShard> others = null;
		synchronized (this) {
			if (size == 0)
				return;
			if (ReadBus == null) {
				ReadBus = MemoReadBus.getBus();
			}
			NodeValueShard shard = new NodeValueShard(this);
			NodeValueIndex index = new NodeValueIndex(shard);

			if (STORESIZE*COMPRESSION_RATIO - size > 0) {
				others = ReadBus.getSparseNodeValueShards(size/COMPRESSION_RATIO);
			}
			if (others != null){
				others.add(0,shard);
				ReadBus.addNodeValueIndex(index, shard);
			} else {
				ReadBus.addNodeValueIndex(index, shard);
			}
			this.nodes.clear();
			size=0;
		}
		if (others != null){
			NodeValueShard.devideAndMerge(others.toArray(new NodeValueShard[0]));
		}
	}

	public ImmutableList<NodeValue> findAll(UUID id) {
		synchronized (this) {
			if (size == 0) return null;
			List<NodeValue> list = nodes.get(id.time);
			if (list != null && !list.isEmpty()) {
				Collections.sort(list);
				for (NodeValue nv: list){
					if (!nv.getId().equals(id)) list.remove(nv);
				}
				return ImmutableList.copyOf(list);
			} else {
				return new ImmutableList.Builder<NodeValue>().build();
			}
		}
	}

	public NodeValue find(UUID id) {
		return findBefore(id, System.currentTimeMillis());
	}

	public NodeValue findBefore(UUID id, Date timestamp) {
		return findBefore(id, timestamp.getTime());
	}

	public NodeValue findBefore(UUID id, long timestamp_long) {
		synchronized (this) {
			if (size == 0) return null;
			if (timestamp_long < oldest)
				return null; // shortcut, will probably not be used...

			List<NodeValue> res = nodes.get(id.time);
			if (res != null && !res.isEmpty()) {
				Collections.sort(res);
				NodeValue result = null;
				Iterator<NodeValue> iter = res.iterator();
				while (iter.hasNext()) {
					NodeValue next = iter.next();
					if (!next.getId().equals(id)) continue; 
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
	}

	public long getOldest() {
		return oldest;
	}

	public long getNewest() {
		return newest;
	}
}
