package com.chap.memo.memoNodes.storage;

import java.util.HashSet;
import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

//TODO: potentially small, in which case we might want to store multiple indexes in one MemoStorable.
public final class NodeValueIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	private Set<UUID> nodeIds;
	private Key shardKey;
	private long oldest;
	private long newest;

	NodeValueIndex() {
	};

	public NodeValueIndex(NodeValueShard shard) {
		nodeIds = new HashSet<UUID>(shard.nodes.keySet());
		shardKey = shard.store("NodeValueShard", shard.newest);
		oldest = shard.oldest;
		newest = shard.newest;
		this.store("NodeValueIndex");
	}

	public static NodeValueIndex load(Key key) {
		return (NodeValueIndex) MemoStorable.load(key);
	}

	public NodeValueShard loadShard() {
		return (NodeValueShard) MemoStorable.load(this.shardKey);
	}

	public String toString() {
		return this.myKey + ": " + oldest + "/" + newest + " : "
				+ nodeIds.size() + " - " + this.nanoTime;
	}

	public Set<UUID> getNodeIds() {
		return nodeIds;
	}

	public void setNodeIds(Set<UUID> nodeIds) {
		this.nodeIds = nodeIds;
	}

	public Key getShardKey() {
		return shardKey;
	}

	public void setShardKey(Key shardKey) {
		this.shardKey = shardKey;
	}

	public long getOldest() {
		return oldest;
	}

	public void setOldest(long oldest) {
		this.oldest = oldest;
	}

	public long getNewest() {
		return newest;
	}

	public void setNewest(long newest) {
		this.newest = newest;
	}
}
