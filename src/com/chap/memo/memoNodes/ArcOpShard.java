package com.chap.memo.memoNodes;

import java.util.ArrayList;
import java.util.HashMap;

import com.eaio.uuid.UUID;

public final class ArcOpShard extends MemoStorable {
	private static final long serialVersionUID = 7712775430540649570L;
	static final int SHARDSIZE= 30000;
	protected int currentSize = 0;
	protected final HashMap<UUID,ArrayList<ArcOp>> parents = new HashMap<UUID,ArrayList<ArcOp>>(SHARDSIZE);
	protected HashMap<UUID,ArrayList<ArcOp>> children = new HashMap<UUID,ArrayList<ArcOp>>(SHARDSIZE);
	
	
	protected void store(ArcOp ops) {
		ArrayList<ArcOp> cur = parents.get(ops.getParent());
		if (cur == null) {
			cur = new ArrayList<ArcOp>(3);
		}
		cur.add(ops);
		synchronized(this){
			parents.put(ops.getParent(), cur);
		}
		
		cur = children.get(ops.getChild());
		if (cur == null) {
			cur = new ArrayList<ArcOp>(3);
		}
		cur.add(ops);
		synchronized(this){
			children.put(ops.getChild(), cur);
		}
		currentSize+=2;
	}
	
	protected ArrayList<ArcOp> getChildOps(UUID id) {
		ArrayList<ArcOp> result =children.get(id);
		if (result == null) result = new ArrayList<ArcOp>(0);
		return result;
	}
	protected ArrayList<ArcOp> getParentOps(UUID id) {
		ArrayList<ArcOp> result =parents.get(id);
		if (result == null) result = new ArrayList<ArcOp>(0);
		return result;	}

}
