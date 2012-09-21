package com.chap.memo.memoNodes.storage;

import java.util.Arrays;

import com.chap.memo.memoNodes.MemoUtils;
import com.chap.memo.memoNodes.model.NodeValue;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

public final class NodeValueIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	
	private final long[] nodeIdArray;
	private final Key shardKey;
	private final long oldest;
	private final long newest;
	
	public NodeValueIndex(NodeValueShard shard) {
		long[] tmp = new long[shard.nodeArray.length];
		NodeValue last = null;
		int count=0;
		for (NodeValue val : shard.nodeArray){
			if (last != null && val.getId().time == last.getId().time){
				continue;
			}
			tmp[count++]=MemoUtils.gettime(val.getId());
			last=val;
		}
		nodeIdArray = Arrays.copyOf(tmp, count);
		shardKey = shard.store("NodeValueShard", shard.newest);
		oldest = shard.oldest;
		newest = shard.newest;
		this.store("NodeValueIndex");
	}
	
	@Override
	public int compareTo(MemoStorable other) {
		if (other instanceof NodeValueIndex){
			NodeValueIndex o = (NodeValueIndex) other;
			return this.newest==o.newest?0:(this.newest>o.newest?1:-1);
		} else {
			return super.compareTo(other);
		}
	}
	public static NodeValueIndex load(Key key) {
		return (NodeValueIndex) MemoStorable.load(key);
	}

	public String toString() {
		return this.myKey + ": " + oldest + "/" + newest + " : "
				+ nodeIdArray.length + " - " + this.nanoTime;
	}

	public boolean contains(UUID uuid){
		int res =MemoUtils.binarySearch(nodeIdArray, MemoUtils.gettime(uuid)); 
//		System.out.println("Searching for :"+uuid+" in index: "+ res);
		return (res>=0);
	}
	
	public Key getShardKey() {
		return shardKey;
	}

	public long getOldest() {
		return oldest;
	}
	public int getSize(){
		return nodeIdArray.length;
	}

	public long getNewest() {
		return newest;
	}

}
