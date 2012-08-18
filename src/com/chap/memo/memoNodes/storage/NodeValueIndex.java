package com.chap.memo.memoNodes.storage;

import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

public final class NodeValueIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	private Set<UUID> nodeIds;
	private Key shardKey;
	private long oldest;
	private long newest;

	NodeValueIndex() {
	};

	public NodeValueIndex(NodeValueShard shard) {
		nodeIds = shard.nodes.keySet();
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
				+ nodeIds.size() + " - " + this.nanoTime;
	}

	public Set<UUID> getNodeIds() {
		return nodeIds;
	}

	public Key getShardKey() {
		return shardKey;
	}

	public long getOldest() {
		return oldest;
	}

	public long getNewest() {
		return newest;
	}

}
