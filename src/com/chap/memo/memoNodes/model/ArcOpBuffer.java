package com.chap.memo.memoNodes.model;

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
	public static final int STORESIZE = 250000;
	public static final int BYTESPEROP = 100; //estimate!
	public static final int COMPRESSION_RATIO = 15;
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
			if (parents.size()*BYTESPEROP >= STORESIZE*COMPRESSION_RATIO) {
				flush();
			}
		}
	}

	public void flush() {
		synchronized (this) {
			if (parents.size() == 0)
				return;
			if (ReadBus == null) {
				ReadBus = MemoReadBus.getBus();
			}
			// Get semi empty shard
			ArcOpShard other = null;
			if (STORESIZE - parents.size()*BYTESPEROP > 0) {
				other = ReadBus.getSparseArcOpShard(STORESIZE - parents.size()*BYTESPEROP);
			}
			ArcOpShard shard = new ArcOpShard(this, other);
			ArcOpIndex index = new ArcOpIndex(shard);
			ReadBus.addOpsIndex(index, shard);
			if (other != null) {
				ArcOpIndex idx = ReadBus.removeArcOpIndexByShard(other
						.getMyKey());
				if (idx != null)
					idx.delete();
				ReadBus.delShard(other);
				other.delete();
			}
			this.parents.clear();
			this.children.clear();
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
//			return ImmutableList.copyOf(children.get(id.time));
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
//			return ImmutableList.copyOf(parents.get(id.time));
		}
	}

}
