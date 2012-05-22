package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import java.util.HashMap;

import com.chap.memo.memoNodes.model.ArcOp;
import com.eaio.uuid.UUID;

public final class ArcOpShard extends MemoStorable {
	private static final long serialVersionUID = 7712775430540649570L;
	public static final int SHARDSIZE = 30000;
	int currentSize = 0;
	final HashMap<UUID, ArrayList<ArcOp>> parents = new HashMap<UUID, ArrayList<ArcOp>>(
			SHARDSIZE);
	HashMap<UUID, ArrayList<ArcOp>> children = new HashMap<UUID, ArrayList<ArcOp>>(
			SHARDSIZE);

	public void store(ArcOp ops) {
		ArrayList<ArcOp> cur = parents.get(ops.getParent());
		if (cur == null) {
			cur = new ArrayList<ArcOp>(3);
		}
		cur.add(ops);
		synchronized (this) {
			parents.put(ops.getParent(), cur);
		}

		cur = children.get(ops.getChild());
		if (cur == null) {
			cur = new ArrayList<ArcOp>(3);
		}
		cur.add(ops);
		synchronized (this) {
			children.put(ops.getChild(), cur);
		}
		currentSize += 2;
	}

	public ArrayList<ArcOp> getChildOps(UUID id) {
		ArrayList<ArcOp> result = children.get(id);
		if (result == null)
			result = new ArrayList<ArcOp>(0);
		return result;
	}

	public ArrayList<ArcOp> getParentOps(UUID id) {
		ArrayList<ArcOp> result = parents.get(id);
		if (result == null)
			result = new ArrayList<ArcOp>(0);
		return result;
	}

	public int getCurrentSize() {
		return currentSize;
	}

	public void setCurrentSize(int currentSize) {
		this.currentSize = currentSize;
	}

	public HashMap<UUID, ArrayList<ArcOp>> getChildren() {
		return children;
	}

	public void setChildren(HashMap<UUID, ArrayList<ArcOp>> children) {
		this.children = children;
	}

	public HashMap<UUID, ArrayList<ArcOp>> getParents() {
		return parents;
	}

}
