package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.ArcOpBuffer;
import com.eaio.uuid.UUID;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ObjectArrays;

public final class ArcOpShard extends MemoStorable {
	private static final long serialVersionUID = 7712775430540649570L;
	ArcOp[] parentArray;
	ArcOp[] childArray;
	
	public transient ImmutableListMultimap<Long,ArcOp> children=null;
	public transient ImmutableListMultimap<Long,ArcOp> parents=null;
	
	public static final transient ArrayListMultimap<Long,ArcOp> template = ArrayListMultimap.create();
	transient boolean init=false;
	transient boolean toDelete=false;
	
	public ArcOpShard(ArcOpShard[] shards){
		ArcOpShard shard = shards[0];
		List<ArcOp> childList = new ArrayList<ArcOp>();
		childList.addAll(Arrays.asList(shard.childArray));
		List<ArcOp> parentList = new ArrayList<ArcOp>();
		parentList.addAll(Arrays.asList(shard.parentArray));
		for (ArcOpShard other : shards){
			if (other.equals(shard)) continue;
			childList.addAll(Arrays.asList(other.childArray));
			parentList.addAll(Arrays.asList(other.parentArray));
		}
		childArray = childList.toArray(new ArcOp[0]);		
		parentArray = parentList.toArray(new ArcOp[0]);
		initMultimaps();
	}
	
	public ArcOpShard(ArcOpBuffer buffer, ArcOpShard other){
		if (other != null){
			System.out.println("Merging shards");
			ArcOp[] par = buffer.getParentOps().toArray(new ArcOp[0]);
			ArcOp[] chld = buffer.getChildOps().toArray(new ArcOp[0]);
			parentArray = ObjectArrays.concat(par, other.parentArray,ArcOp.class);
			childArray = ObjectArrays.concat(chld, other.childArray,ArcOp.class);
		} else {
			parentArray = buffer.getParentOps().toArray(new ArcOp[0]);
			childArray = buffer.getChildOps().toArray(new ArcOp[0]);
		}
		initMultimaps();
	}
	
	public void initMultimaps(){
		if (!init){
			ImmutableListMultimap.Builder<Long,ArcOp> childBuilder = new ImmutableListMultimap.Builder<Long,ArcOp>();
			for (ArcOp ops: Arrays.asList(childArray)){
				if (ops == null) break;
		 		childBuilder.put(ops.getChild().time,ops);
			}
			children = childBuilder.build();
			ImmutableListMultimap.Builder<Long,ArcOp> parentBuilder = new ImmutableListMultimap.Builder<Long,ArcOp>();
			for (ArcOp ops: Arrays.asList(parentArray)){
				if (ops == null) break;
				parentBuilder.put(ops.getParent().time,ops);
			}
			parents = parentBuilder.build();
			init=true;
		}
	}

	public ImmutableList<ArcOp> getChildOps(UUID id) {
		initMultimaps();
		return children.get(id.time);
	}

	public ImmutableList<ArcOp> getParentOps(UUID id) {
		initMultimaps();
		return parents.get(id.time);
	}
	@Override
	public int getSize(){
		return parentArray.length * ArcOpBuffer.BYTESPEROP;
	}
}
