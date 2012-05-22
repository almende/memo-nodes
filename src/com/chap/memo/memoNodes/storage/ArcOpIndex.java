package com.chap.memo.memoNodes.storage;

import java.util.Set;
import java.util.HashSet;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;

//TODO: potentially small, in which case we might want to store multiple indexes in one MemoStorable.
public final class ArcOpIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	private Set<UUID> parents;
	private Set<UUID> children;
	private Key shardKey;

	ArcOpIndex() {
	}

	public ArcOpIndex(ArcOpShard ops) {
		parents = new HashSet<UUID>(ops.parents.keySet());
		children = new HashSet<UUID>(ops.children.keySet());
		shardKey = ops.store("ArcOpShard");
		this.store("ArcOpIndex");
	}

	public static ArcOpIndex load(Key key) {
		return (ArcOpIndex) MemoStorable.load(key);
	}

	public ArcOpShard loadShard() {
		return (ArcOpShard) MemoStorable.load(shardKey);
	}

	public Set<UUID> getParents() {
		return parents;
	}

	public void setParents(Set<UUID> parents) {
		this.parents = parents;
	}

	public Set<UUID> getChildren() {
		return children;
	}

	public void setChildren(Set<UUID> children) {
		this.children = children;
	}

	public Key getShardKey() {
		return shardKey;
	}

	public void setShardKey(Key shardKey) {
		this.shardKey = shardKey;
	}
}
