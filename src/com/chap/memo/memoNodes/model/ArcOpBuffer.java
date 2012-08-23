package com.chap.memo.memoNodes.model;

import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.storage.ArcOpIndex;
import com.chap.memo.memoNodes.storage.ArcOpShard;
import com.eaio.uuid.UUID;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

public class ArcOpBuffer {
	public final static int SHARDSIZE = 30000;
	MemoReadBus ReadBus;

	public static final ArrayListMultimap<UUID, ArcOp> template = ArrayListMultimap
			.create();
	public ListMultimap<UUID, ArcOp> children = ArrayListMultimap.create(template);
	public ListMultimap<UUID, ArcOp> parents = ArrayListMultimap.create(template);

	public void store(ArcOp ops) {
		synchronized (this) {
			parents.put(ops.getParent(), ops);
			children.put(ops.getChild(), ops);
			if (parents.size() >= SHARDSIZE) {
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
			if (SHARDSIZE - parents.size() > 0) {
				other = ReadBus.getSparseArcOpShard(SHARDSIZE - parents.size());
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

	public ImmutableList<ArcOp> getChildOps(UUID id) {
		synchronized(this){
			return ImmutableList.copyOf(children.get(id));
		}
	}

	public ImmutableList<ArcOp> getParentOps(UUID id) {
		synchronized(this){
			return ImmutableList.copyOf(parents.get(id));
		}
	}

}
