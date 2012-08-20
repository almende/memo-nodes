package com.chap.memo.memoNodes.storage;

import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

//TODO: potentially small, in which case we might want to store multiple indexes in one MemoStorable.
public final class ArcOpIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	private Set<UUID> parents; //TODO: should be synchronized as well?
	private Set<UUID> children;
	private Key shardKey;

	ArcOpIndex() {
	}
	
	//Deep copy constructor
	public ArcOpIndex(ArcOpShard ops) {
		parents = ops.parents.keySet();
		children = ops.children.keySet();
		shardKey = ops.store("ArcOpShard");
		this.store("ArcOpIndex");
	}

	public static ArcOpIndex load(Key key) {
		return (ArcOpIndex) MemoStorable.load(key);
	}

	public Set<UUID> getParents() {
		return parents;
	}

	public Set<UUID> getChildren() {
		return children;
	}

	public Key getShardKey() {
		return shardKey;
	}
	public int getSize(){
		return parents.size();
	}
}
