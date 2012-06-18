package com.chap.memo.memoNodes.bus;

import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.storage.ArcOpIndex;
import com.chap.memo.memoNodes.storage.ArcOpShard;
import com.chap.memo.memoNodes.storage.NodeValueIndex;
import com.chap.memo.memoNodes.storage.NodeValueShard;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

public class MemoWriteBus {
	private final static MemoWriteBus bus = new MemoWriteBus();
	static final DatastoreService datastore = DatastoreServiceFactory
			.getDatastoreService();
	NodeValueShard values;
	ArcOpShard ops;
	MemoReadBus ReadBus;

	private MemoWriteBus() {
		values = new NodeValueShard();
		ops = new ArcOpShard();
		ReadBus = MemoReadBus.getBus();
	};

	public static MemoWriteBus getBus() {
		return bus;
	}

	public static void emptyDB() {
		// create one big cleanup query
		String[] types = { "NodeValueIndex", "ArcOpIndex", "NodeValueShard",
				"ArcOpShard" };
		for (String type : types) {
			Query q = new Query(type).setKeysOnly();
			PreparedQuery pq = datastore.prepare(q);
			// int count = pq.countEntities();
			// System.out.println("Deleting :"+count+" entries of type:"+type);
			for (Entity res : pq.asIterable()) {
				datastore.delete(res.getKey());
				try {
					datastore.get(res.getKey());
				} catch (EntityNotFoundException e) {
				}
			}
		}
		MemoReadBus.getBus().loadIndexes(true, 0);
		System.out.println("Database cleared!");
	}

	public void flush() {
		flushValues();
		flushOps();
		ReadBus.updateIndexes();
	}

	public void flushValues() {
		synchronized (values.getNodes()) {
			if (values.getNodes().size() > 0) {
				NodeValueIndex index = new NodeValueIndex(values);
				ReadBus.addValueIndex(index, values);
				values = new NodeValueShard();
			}
		}
//		System.out.println("ks:"+MemoStorable.knownStorables + ":"+MemoReadBus.NodeValueShards.size()+":"+MemoReadBus.ArcOpShards.size()+":"+ReadBus.NodeValueIndexes.size()+":"+ReadBus.ArcOpIndexes.size()+":"+NodeValue.knownNodeValues+":"+ArcOp.knownOps);
	}

	public void flushOps() {
		synchronized (ops) {
			if (ops.getCurrentSize() > 0) {
//				long start = System.currentTimeMillis();
				ArcOpIndex index = new ArcOpIndex(ops);
//				long middle = System.currentTimeMillis();
				ReadBus.addOpsIndex(index, ops);
//				System.out.println(".:"+(middle-start)+":"+(System.currentTimeMillis()-middle));
				ops = new ArcOpShard();
			}
		}
		//System.out.println("ks:"+MemoStorable.knownStorables + ":"+MemoReadBus.NodeValueShards.size()+":"+MemoReadBus.ArcOpShards.size()+":"+ReadBus.NodeValueIndexes.size()+":"+ReadBus.ArcOpIndexes.size()+":"+NodeValue.knownNodeValues+":"+ArcOp.knownOps);
	}

	public NodeValue store(UUID id, byte[] value) {
		long now = System.currentTimeMillis();
		NodeValue result = new NodeValue(id, value, now);
		values.store(result);
		ReadBus.lastValueChange = now;
		if (values.getCurrentSize() >= NodeValueShard.SHARDSIZE) {
//			long start = System.currentTimeMillis();
			flushValues();
//			System.out.println("FlushValues took: "+(System.currentTimeMillis()-start)+" ms");
		}
		return result;
	}

	public void store(ArcOp op) {
		ops.store(op);
		ReadBus.lastOpsChange = System.currentTimeMillis();
		if (ops.getCurrentSize() >= ArcOpShard.SHARDSIZE) {
//			long start = System.currentTimeMillis();
			flushOps();
//			System.out.println("flushOps took: "+(System.currentTimeMillis()-start)+" ms");
		}
	}
}
