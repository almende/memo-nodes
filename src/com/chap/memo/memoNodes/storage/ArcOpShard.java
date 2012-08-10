package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import java.util.List;

import com.chap.memo.memoNodes.model.ArcOp;
import com.eaio.uuid.UUID;
import com.google.common.collect.ArrayListMultimap;

public final class ArcOpShard extends MemoStorable {
	private static final long serialVersionUID = 7712775430540649570L;
	public static final int SHARDSIZE = 15000;
	int currentSize = 0;
	
	final ArrayListMultimap<UUID,ArcOp> parents = ArrayListMultimap.create();
	final ArrayListMultimap<UUID,ArcOp> children = ArrayListMultimap.create();

	public void store(ArcOp ops) {
		parents.put(ops.getParent(),ops);
		children.put(ops.getChild(),ops);
		currentSize++;
	}

	public List<ArcOp> getChildOps(UUID id) {
		List<ArcOp> result = children.get(id);
		if (result == null)
			result = new ArrayList<ArcOp>(0);
		return result;
	}

	public List<ArcOp> getParentOps(UUID id) {
		List<ArcOp> result = parents.get(id);
		if (result == null)
			result = new ArrayList<ArcOp>(0);
		return result;
	}

	public int getCurrentSize() {
		return currentSize;
	}
}
