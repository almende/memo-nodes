package com.chap.memo.memoNodes.NewImpl;

import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

//TODO: potentially small, in which case we might want to store multiple indexes in one MemoStorable.
public final class ArcOpIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	final Set<UUID> parents;
	final Set<UUID> children;
	final Key shardKey;
	Key myKey;
	
	public ArcOpIndex(ArcOpShard ops){
		parents = ops.parents.keySet();
		children = ops.children.keySet();
		shardKey = ops.store("ArcOpShard");
		myKey = this.store("ArcOpIndex");
	}
	public static ArcOpIndex load(Key key){
		return (ArcOpIndex) MemoStorable.load(key);
	}
	public ArcOpShard loadShard(){
		return (ArcOpShard) MemoStorable.load(shardKey);
	}
}

