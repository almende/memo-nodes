package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import org.apache.commons.collections.map.MultiValueMap;

import com.chap.memo.memoNodes.model.ArcOp;
import com.eaio.uuid.UUID;

public final class ArcOpShard extends MemoStorable {
	private static final long serialVersionUID = 7712775430540649570L;
	public static final int SHARDSIZE = 15000;
	int currentSize = 0;
	
	final MultiValueMap<UUID,ArcOp> parents = new MultiValueMap<UUID,ArcOp>();
	final MultiValueMap<UUID,ArcOp> children = new MultiValueMap<UUID,ArcOp>();

	public void store(ArcOp ops) {
		parents.put(ops.getParent(),ops);
		children.put(ops.getChild(),ops);
		currentSize++;
	}

	public ArrayList<ArcOp> getChildOps(UUID id) {
		ArrayList<ArcOp> result = (ArrayList<ArcOp>) children.getCollection(id);
		if (result == null)
			result = new ArrayList<ArcOp>(0);
		return result;
	}

	public ArrayList<ArcOp> getParentOps(UUID id) {
		ArrayList<ArcOp> result = (ArrayList<ArcOp>) parents.getCollection(id);
		if (result == null)
			result = new ArrayList<ArcOp>(0);
		return result;
	}

	public int getCurrentSize() {
		return currentSize;
	}
}
