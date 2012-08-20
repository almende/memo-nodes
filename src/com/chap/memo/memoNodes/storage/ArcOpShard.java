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
	
	public transient ImmutableListMultimap<UUID,ArcOp> children;
	public transient ImmutableListMultimap<UUID,ArcOp> parents;
	
	public static final ArrayListMultimap<UUID,ArcOp> template = ArrayListMultimap.create();
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
			ArcOp[] par = buffer.parents.values().toArray(new ArcOp[0]);
			ArcOp[] chld = buffer.children.values().toArray(new ArcOp[0]);
			parentArray = ObjectArrays.concat(par, other.parentArray,ArcOp.class);
			childArray = ObjectArrays.concat(chld, other.childArray,ArcOp.class);
		} else {
			parentArray = buffer.parents.values().toArray(new ArcOp[0]);
			childArray = buffer.children.values().toArray(new ArcOp[0]);
		}
		initMultimaps();
	}
	
	private void initMultimaps(){
		if (!init){
			ImmutableListMultimap.Builder<UUID,ArcOp> childBuilder = new ImmutableListMultimap.Builder<UUID,ArcOp>();
			for (ArcOp ops: Arrays.asList(childArray)){
				if (ops == null) break;
		 		childBuilder.put(ops.getChild(),ops);
			}
			children = childBuilder.build();
			ImmutableListMultimap.Builder<UUID,ArcOp> parentBuilder = new ImmutableListMultimap.Builder<UUID,ArcOp>();
			for (ArcOp ops: Arrays.asList(parentArray)){
				if (ops == null) break;
				parentBuilder.put(ops.getParent(),ops);
			}
			parents = parentBuilder.build();
			init=true;
		}
	}

	public ImmutableList<ArcOp> getChildOps(UUID id) {
		initMultimaps();
		return children.get(id);
	}

	public ImmutableList<ArcOp> getParentOps(UUID id) {
		initMultimaps();
		return parents.get(id);
	}
	@Override
	public int getSize(){
		return parentArray.length;
	}
}
