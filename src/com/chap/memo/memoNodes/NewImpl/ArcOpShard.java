package com.chap.memo.memoNodes.NewImpl;

import java.util.ArrayList;
import java.util.HashMap;

import com.eaio.uuid.UUID;

public final class ArcOpShard extends MemoStorable {
	private static final long serialVersionUID = 7712775430540649570L;
	static final int SHARDSIZE= 40000;
	final HashMap<UUID,ArrayList<ArcOp>> parents = new HashMap<UUID,ArrayList<ArcOp>>(SHARDSIZE);
	final HashMap<UUID,ArrayList<ArcOp>> children = new HashMap<UUID,ArrayList<ArcOp>>(SHARDSIZE);
	
	public void store(ArcOp ops) {
		ArrayList<ArcOp> cur = parents.get(ops.getParent());
		if (cur != null) {
			int size = cur.size();
			boolean found = false;
			for (int i = 0; i < size; i++) {
				long comp = cur.get(i).getTimestamp_long();
				if (comp <= ops.getTimestamp_long())continue;
				cur.add(i,ops);
				found = true;
				break;
			}
			if (!found) {
				cur.add(ops);
			}
		} else {
			cur = new ArrayList<ArcOp>(3);
			cur.add(ops);
		}
		parents.put(ops.getParent(), cur);
		
		cur = children.get(ops.getChild());
		if (cur != null) {
			int size = cur.size();
			boolean found = false;
			for (int i = 0; i < size; i++) {
				long comp = cur.get(i).getTimestamp_long();
				if (comp <= ops.getTimestamp_long())continue;
				cur.add(i,ops);
				found = true;
				break;
			}
			if (!found) {
				cur.add(ops);
			}
		} else {
			cur = new ArrayList<ArcOp>(3);
			cur.add(ops);
		}
		children.put(ops.getChild(), cur);
		
	}
	
	public ArrayList<ArcOp> getChildOps(UUID id) {
		ArrayList<ArcOp> result =children.get(id);
		if (result == null) result = new ArrayList<ArcOp>(0);
		return result;
	}
	public ArrayList<ArcOp> getParentOps(UUID id) {
		ArrayList<ArcOp> result =parents.get(id);
		if (result == null) result = new ArrayList<ArcOp>(0);
		return result;	}

}
