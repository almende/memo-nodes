package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.model.NodeValueBuffer;
import com.eaio.uuid.UUID;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ObjectArrays;

public final class NodeValueShard extends MemoStorable {
	private static final long serialVersionUID = 7295820980658238258L;
	long oldest = 0;
	long newest = 0;
	
	NodeValue[] nodeArray;
	
	public transient ImmutableListMultimap<UUID,NodeValue> nodes;
	transient boolean init=false;
	
	public NodeValueShard(NodeValueBuffer buffer, NodeValueShard other){
		List<NodeValue> list;
		if (other != null){
			System.out.println("Merging shards");
			NodeValue[] nod = buffer.nodes.values().toArray(new NodeValue[0]);
			list = Arrays.asList(ObjectArrays.concat(nod, other.nodeArray,NodeValue.class));
			newest = Math.max(buffer.getNewest(), other.newest);
			oldest = Math.min(buffer.getOldest(), other.oldest);
		} else {
			list = new ArrayList<NodeValue>(buffer.nodes.values());
			newest = buffer.getNewest();
			oldest = buffer.getOldest();
		}
		Collections.sort(list);
		nodeArray=list.toArray(new NodeValue[0]);
		initMultimaps();	
	}
	
	private void initMultimaps(){
		if (!init){
			ImmutableListMultimap.Builder<UUID,NodeValue> nodesBuilder = new ImmutableListMultimap.Builder<UUID,NodeValue>();
			for (NodeValue nv: Arrays.asList(nodeArray)){
				nodesBuilder.put(nv.getId(),nv);
			}
			nodes = nodesBuilder.build();
			init=true;
		}
	}

	public ImmutableList<NodeValue> findAll(UUID id) {
		return nodes.get(id);
	}

	public NodeValue find(UUID id) {
		return findBefore(id, System.currentTimeMillis());
	}

	public NodeValue findBefore(UUID id, Date timestamp) {
		return findBefore(id, timestamp.getTime());
	}

	public NodeValue findBefore(UUID id, long timestamp_long) {
		initMultimaps();
		if (timestamp_long < oldest)
			return null; // shortcut, will probably not be used...

		List<NodeValue> res=nodes.get(id);
		if (res != null && !res.isEmpty()) {		
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
