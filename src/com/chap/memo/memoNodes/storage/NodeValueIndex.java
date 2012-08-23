package com.chap.memo.memoNodes.storage;

import java.util.Arrays;
import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

public final class NodeValueIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	
	private long[] nodeIdArray;
	private Key shardKey;
	private long oldest;
	private long newest;
	
	NodeValueIndex() {
	};

	public NodeValueIndex(NodeValueShard shard) {
		Set<UUID> set = shard.nodes.keySet();
		nodeIdArray = new long[set.size()];
		int i=0;
		for (UUID node: set){
			nodeIdArray[i++]=node.time;
		}
		Arrays.sort(nodeIdArray);
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
		return (Arrays.binarySearch(nodeIdArray, uuid.getTime())>=0);
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
