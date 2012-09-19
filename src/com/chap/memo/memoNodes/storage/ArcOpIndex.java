package com.chap.memo.memoNodes.storage;

import java.util.Arrays;

import com.chap.memo.memoNodes.MemoUtils;
import com.chap.memo.memoNodes.model.ArcOp;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;


public final class ArcOpIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	private final long[] parentArray; 
	private final long[] childrenArray;
	private final Key shardKey;
	private final boolean hasParentRoot;
	private final boolean hasChildRoot;
	
	//Deep copy constructor
	public ArcOpIndex(ArcOpShard ops) {
		long[] tmp = new long[ops.parentArray.length];
		ArcOp last = null;
		int count=0;
		hasParentRoot = ops.hasParentRoot();
		for (ArcOp op : ops.parentArray){
			if (last != null && op.getParent().time == last.getParent().time){
				continue;
			}
			tmp[count++]=op.getParentTime();
			last=op;
		}
		parentArray = Arrays.copyOf(tmp, count);
		
		tmp = new long[ops.childArray.length];
		last = null;
		count=0;
		hasChildRoot = ops.hasChildRoot();
		for (ArcOp op : ops.childArray){
			if (last != null && op.getChild().time == last.getChild().time){
				continue;
			}
			tmp[count++]=op.getChildTime();
			last=op;
		}
		childrenArray = Arrays.copyOf(tmp, count);
		
		shardKey = ops.store("ArcOpShard");
		long time = Math.min(parentArray.length>0?parentArray[0]:System.currentTimeMillis(), childrenArray.length>0?childrenArray[0]:System.currentTimeMillis());
//		System.out.println("Storing index with time:"+time+" "+new Date(time).toString());
		this.store("ArcOpIndex",time);
	}

	public static ArcOpIndex load(Key key) {
		return (ArcOpIndex) MemoStorable.load(key);
	}

	public boolean containsParent(UUID uuid){
		if (uuid.time == 0) return hasParentRoot;
		return (MemoUtils.binarySearch(parentArray,MemoUtils.gettime(uuid)) >= 0);
	}
	public boolean containsChild(UUID uuid){
		if (uuid.time == 0) return hasChildRoot;
		return (MemoUtils.binarySearch(childrenArray,MemoUtils.gettime(uuid)) >= 0);
	}

	public Key getShardKey() {
		return shardKey;
	}
	public int getSize(){
		return parentArray.length;
	}
}

