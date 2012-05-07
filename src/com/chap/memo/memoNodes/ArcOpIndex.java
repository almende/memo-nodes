package com.chap.memo.memoNodes;

import java.util.Set;
import java.util.HashSet;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

//TODO: potentially small, in which case we might want to store multiple indexes in one MemoStorable.
public final class ArcOpIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	Set<UUID> parents;
	Set<UUID> children;
	Key shardKey;
	
	protected ArcOpIndex(){}
	protected ArcOpIndex(ArcOpShard ops){
		parents = new HashSet<UUID>(ops.parents.keySet());
		children = new HashSet<UUID>(ops.children.keySet());
		shardKey = ops.store("ArcOpShard");
		this.store("ArcOpIndex");
	}
	protected static ArcOpIndex load(Key key){
		return (ArcOpIndex) MemoStorable.load(key);
	}
	protected ArcOpShard loadShard(){
		return (ArcOpShard) MemoStorable.load(shardKey);
	}
}

