package com.chap.memo.memoNodes.storage;

import java.util.Arrays;
import java.util.Set;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Key;


public final class ArcOpIndex extends MemoStorable {
	private static final long serialVersionUID = -4925678482726158446L;
	private long[] parentArray; 
	private long[] childrenArray;
	private Key shardKey;

	ArcOpIndex() {}
	
	//Deep copy constructor
	public ArcOpIndex(ArcOpShard ops) {
		Set<Long> parentsSet = ops.parents.keySet();
		Set<Long> childrenSet = ops.children.keySet();
		parentArray = new long[parentsSet.size()];
		int i=0;
		for (Long parent: parentsSet){
			parentArray[i++]=parent;
		}
		Arrays.sort(parentArray);
		childrenArray = new long[childrenSet.size()];
		i=0;
		for (Long child: childrenSet){
			childrenArray[i++]=child;
		}
		Arrays.sort(childrenArray);
		shardKey = ops.store("ArcOpShard");
		this.store("ArcOpIndex");
	}

	public static ArcOpIndex load(Key key) {
		return (ArcOpIndex) MemoStorable.load(key);
	}

	public boolean containsParent(UUID uuid){
		return (Arrays.binarySearch(parentArray,uuid.getTime()) >= 0);
	}
	public boolean containsChild(UUID uuid){
		return (Arrays.binarySearch(childrenArray,uuid.getTime()) >= 0);
	}

	public Key getShardKey() {
		return shardKey;
	}
	public int getSize(){
		return parentArray.length;
	}
}

