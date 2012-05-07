package com.chap.memo.memoNodes;

import java.util.HashSet;
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
	
	public NodeValueIndex(){};
	protected NodeValueIndex(NodeValueShard shard){
		nodeIds = new HashSet<UUID>(shard.nodes.keySet());
		shardKey = shard.store("NodeValueShard",shard.newest);
		oldest = shard.oldest;
		newest = shard.newest;
		this.store("NodeValueIndex");
	}
	protected static NodeValueIndex load(Key key){
		return (NodeValueIndex) MemoStorable.load(key);
	}
	protected NodeValueShard loadShard(){
		return (NodeValueShard) MemoStorable.load(this.shardKey);
	}
	public String toString(){
		return this.myKey+": "+oldest+"/"+newest+" : "+nodeIds.size()+" - "+this.nanoTime;
	}
}
