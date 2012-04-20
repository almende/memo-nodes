package com.chap.memo.memoNodes.NewImpl;

import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

//TODO: potentially small, in which case we might want to store multiple indexes in one MemoStorable.
public final class ArcOpIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	Set<UUID> parents;
	Set<UUID> children;
	Key shardKey;
	
	public ArcOpIndex(){}
	public ArcOpIndex(ArcOpShard ops){
		parents = ops.parents.keySet();
		children = ops.children.keySet();
		shardKey = ops.store("ArcOpShard");
		this.store("ArcOpIndex");
	}
	public static ArcOpIndex load(Key key){
		return (ArcOpIndex) MemoStorable.load(key);
	}
	public ArcOpShard loadShard(){
		return (ArcOpShard) MemoStorable.load(shardKey);
	}
}
