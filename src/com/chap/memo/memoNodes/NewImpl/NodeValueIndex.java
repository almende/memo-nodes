package com.chap.memo.memoNodes.NewImpl;

import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

//TODO: potentially small, in which case we might want to store multiple indexes in one MemoStorable.
public final class NodeValueIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	Set<UUID> nodeIds;
	Key shardKey;
	long oldest;
	long newest;
	Key myKey;
	
	public NodeValueIndex(NodeValueShard shard){
		nodeIds = shard.nodes.keySet();
		shardKey = shard.store("NodeValueShard");
		oldest = shard.oldest;
		newest = shard.newest;
		myKey = this.store("NodeValueIndex");
	}
	public static NodeValueIndex load(Key key){
		return (NodeValueIndex) MemoStorable.load(key);
	}
	public NodeValueShard loadShard(){
		return (NodeValueShard) MemoStorable.load(this.shardKey);
	}
}
