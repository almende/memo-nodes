package com.chap.memo.memoNodes.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.storage.ArcOpIndex;
import com.chap.memo.memoNodes.storage.ArcOpShard;
import com.eaio.uuid.UUID;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

public class ArcOpBuffer {
	public static final int STORESIZE = 25000;
	public static final int BYTESPEROP = 25; //estimate!
	public static final int COMPRESSION_RATIO = 8;
	MemoReadBus ReadBus;

	public static final ArrayListMultimap<Long, ArcOp> template = ArrayListMultimap
			.create();
	ListMultimap<Long, ArcOp> children = ArrayListMultimap.create(template);
	ListMultimap<Long, ArcOp> parents = ArrayListMultimap.create(template);	
	
	public void clear(){
		synchronized(this){
			this.children.clear();
			this.parents.clear();
		}
	}
	
	public void store(ArcOp ops) {
		synchronized (this) {
			parents.put(ops.getParent().time, ops);
			children.put(ops.getChild().time, ops);
			if (parents.size()*2*BYTESPEROP >= STORESIZE*COMPRESSION_RATIO) {
				flush();
			}
		}
	}

	public void flush() {
		ArrayList<ArcOpShard> others = null;
		synchronized (this) {
			if (parents.size() == 0)
				return;
			if (ReadBus == null) {
				ReadBus = MemoReadBus.getBus();
			}
			ArcOpShard shard = new ArcOpShard(this);
			ArcOpIndex index = new ArcOpIndex(shard);
			
			// Get semi empty shards
			if (STORESIZE*COMPRESSION_RATIO - parents.size()*2*BYTESPEROP > 0) {
				others = ReadBus.getSparseArcOpShards(parents.size()*2*BYTESPEROP/COMPRESSION_RATIO);
			}
			if (others != null){
				others.add(0,shard);
				ReadBus.addOpsIndex(index, shard);
			} else {
				ReadBus.addOpsIndex(index, shard);
			}
			this.parents.clear();
			this.children.clear();
		}
		if (others != null){
			ArcOpShard.devideAndMerge(others.toArray(new ArcOpShard[0]));
		}
	}
	public ImmutableList<ArcOp> getChildOps(){
		synchronized(this){
			return ImmutableList.copyOf(children.values());
		}
	}
	public ImmutableList<ArcOp> getChildOps(UUID id) {
		synchronized(this){
			List<ArcOp> list = children.get(id.time);
			Iterator<ArcOp> iter = list.iterator();
			while (iter.hasNext()){
				ArcOp op = iter.next();
				if (!op.getChild().equals(id)) iter.remove();
			}
			return ImmutableList.copyOf(list);
		}
	}

	public ImmutableList<ArcOp> getParentOps(){
		synchronized(this){
			return ImmutableList.copyOf(parents.values());
		}
	}
	public ImmutableList<ArcOp> getParentOps(UUID id) {
		synchronized(this){
			List<ArcOp> list = parents.get(id.time);
			Iterator<ArcOp> iter = list.iterator();
			while (iter.hasNext()){
				ArcOp op = iter.next();
				if (!op.getParent().equals(id)) iter.remove();
			}
			return ImmutableList.copyOf(list);
		}
	}

}
