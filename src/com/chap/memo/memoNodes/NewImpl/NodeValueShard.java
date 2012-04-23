package com.chap.memo.memoNodes.NewImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import com.eaio.uuid.UUID;

public final class NodeValueShard extends MemoStorable{
	private static final long serialVersionUID = 7295820980658238258L;
	static final int SHARDSIZE= 40000;
	long oldest=0;
	long newest=0;
	
	final HashMap<UUID,ArrayList<NodeValue>> nodes = new HashMap<UUID,ArrayList<NodeValue>>(SHARDSIZE);
		
	public void store(NodeValue nodeVal) {
		ArrayList<NodeValue> cur = nodes.get(nodeVal.getId());
		if (cur != null) {
			System.out.println("Nodes get: "+nodeVal.getId()+":"+cur.size());
			int size = cur.size();
			boolean found = false;
			for (int i = 0; i < size; i++) {
				long comp = cur.get(i).getTimestamp_long();
				if (comp < nodeVal.getTimestamp_long())continue;
				cur.add(i,nodeVal);
				found = true;
				System.out.println("Add at"+i);
				
				break;
			}
			if (!found) {
				System.out.println("Add to end");
				cur.add(nodeVal);
			}
		} else {
			System.out.println("Nodes get: "+nodeVal.getId()+":new cur");
			cur = new ArrayList<NodeValue>(3);
			cur.add(nodeVal);
		}
		nodes.put(nodeVal.getId(), cur);
		System.out.println("Nodes.put: "+nodeVal.getId()+":"+cur.size());
		if (newest == 0 || nodeVal.getTimestamp_long()>newest) newest = nodeVal.getTimestamp_long();
		if (oldest == 0 || nodeVal.getTimestamp_long()<oldest) oldest = nodeVal.getTimestamp_long();
	}
	public ArrayList<NodeValue> findAll(UUID id) {
		return nodes.get(id);
	}
	public NodeValue find(UUID id) {
		return findBefore(id,new Date());
	}
	public NodeValue findBefore(UUID id, Date timestamp) {
		return findBefore(id,timestamp.getTime());
	}	
	
	public NodeValue findBefore(UUID id, long timestamp_long){
		if (timestamp_long < oldest) return null; //shortcut, will probably not be used...
		
		ArrayList<NodeValue> res = nodes.get(id);
		if (res != null && !res.isEmpty()) {
			NodeValue result = null;
			Iterator<NodeValue> iter = res.iterator();
			while (iter.hasNext()) {
				NodeValue next = iter.next();
				if (next.getTimestamp_long() <= timestamp_long) {
					if (result == null
							|| next.getTimestamp_long() > result.getTimestamp_long()) {
						result = next;
					}
				}
			}
			return result;
		}
		return null;
	}
}
